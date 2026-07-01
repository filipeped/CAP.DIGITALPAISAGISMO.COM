# AUDITORIA-CAPI.md — Proxy CAPI (cap.digitalpaisagismo.com)

> Documento de referência do proxy de eventos Meta (Conversions API).
> **Leia isto ANTES de auditar `pages/api/events.ts` de novo** — vários pontos já foram
> verificados e decididos. Não reacusar o que está na seção "JÁ VERIFICADO".

- **Data da auditoria:** 01/07/2026
- **Versão:** V9.3 (era V9.2)
- **Papel:** recebe eventos do painel (`DIGITALPAISAGISMO.COM.03`) e do bot (n8n),
  **hasheia a PII (SHA-256)**, deduplica e repassa pra Meta Graph API v21.0.
- **Negócio:** campanhas **NÃO** são Click-to-WhatsApp (CTWA). Funil é **WEB**
  (site → proposta → compra). CTWA **não se aplica** hoje.

---

## ✅ CORRIGIDO nesta rodada (V9.2 → V9.3, 01/07/2026)

Tudo em `pages/api/events.ts`:

1. **[CRÍTICO] Dedup em 2 fases (lock in-flight → confirma na entrega).**
   Antes o `event_id` era gravado no Redis (24h) **antes** do POST pra Meta. Se a Meta
   falhasse, o retry do caller batia como "duplicado" e o **evento se perdia** silenciosamente.
   Agora:
   - `isDuplicateEvent()` só **adquire um lock curto (90s)** via `SET NX`.
   - `confirmDelivery()` estende o TTL pra **24h** só após a Meta responder **2xx**.
   - `releaseEvents()` **apaga** o lock se a Meta falhar (erro/timeout) → o retry **passa a valer**.
   - Corrida de 2 requests idênticos: o 2º pega o lock e é tratado como dup; a Meta ainda
     deduplica nativa por `event_id`, então nunca conta 2x.
   - Nota: em **timeout** o evento pode ter chegado na Meta; o release permite reenvio, mas
     a Meta deduplica por `event_id` — no pior caso reenvia 1x deduplicado (melhor que perder).
2. **Valor fictício de Lead REMOVIDO.** `generateDeterministicValue` (inventava R$10-100 em
   Lead sem value) foi apagada. Lead sem value agora vai **sem value** — mais honesto pra Meta.
3. **Auth server-to-server fail-open** (header `x-internal-secret`). Ver "Env vars".
4. **Telefone:** testa "já hasheado" **antes** de `replace(/\D/)` (corrige double-hash de
   telefone pré-hasheado) + **rede de segurança do DDI 55** (nº nacional de 10-11 dígitos
   ganha o `55`).
5. **`access_token` fora da URL:** agora vai no **corpo** da request (a CAPI `/events` aceita),
   pra não vazar em log de proxy/CDN que capture a URL.

---

## Env vars

| Env                       | Papel |
|---------------------------|-------|
| `META_PIXEL_ID`           | Pixel/Dataset ID (via env — não hardcoded) |
| `META_ACCESS_TOKEN`       | Token da CAPI (via env — agora vai no body, não na URL) |
| `UPSTASH_REDIS_REST_URL`  | Redis de dedup distribuído (fallback: memória) |
| `UPSTASH_REDIS_REST_TOKEN`| idem |
| `INTERNAL_SECRET` ⚠️      | Segredo compartilhado com os callers (.03 e n8n) |

⚠️ **`INTERNAL_SECRET` — precisa do OK do Filipe pra ativar.** Hoje é **fail-open**: sem a env,
o proxy aceita tudo (só loga aviso) — produção não quebra. Pra ativar, o **mesmo valor** tem
que estar em **todos os callers** (.03 já manda o header quando a env dele estiver setada; o
**n8n precisa ser configurado** pra mandar `x-internal-secret`) **e** aqui. **Não ativar só de
um lado** — senão os eventos de quem não manda o header tomam 401 e somem.

---

## 🔒 JÁ VERIFICADO — NÃO REACUSAR EM AUDITORIAS FUTURAS

- **HASH SHA-256:** CONFIRMADO correto.
  - Email: `trim + lowercase` → SHA256. ✅
  - Telefone: só dígitos (+ DDI 55 como rede de segurança) → SHA256. ✅
  - Nome/sobrenome (`fn`/`ln`): `trim + lowercase` → SHA256. ✅
  - `ct`/`st`/`zp`/`country`: `trim` → SHA256 (a normalização minúsculo/sem-acento é feita
    no caller `.03` via `normGeo` — **combinado, por design**). ✅
  - Proteção anti-double-hash: cada campo checa se já é 64-hex antes de hashear. ✅
- **CTWA / `ctwa_clid` / `ln` / `messaging_channel`:** o proxy **já repassa** todos (commits
  6d0483d/7d5ac0e). Em `action_source='business_messaging'` ele corretamente **omite**
  `event_source_url`/`client_ip`/`client_user_agent` (a Meta rejeita) e **repassa**
  `messaging_channel`. Como o funil é **WEB**, esse caminho fica dormente — **não é problema.**
- **`messaging_channel` obrigatório em `business_messaging`:** o proxy **não dropa** o campo
  (repassa se presente). O caller `.03` seta junto com o `business_messaging`. **Sem risco.**
- **PII crua recebida do .03/n8n:** é **POR DESIGN** — o proxy é o ponto ÚNICO que hasheia,
  pra normalização idêntica entre bot e painel. Chega por HTTPS. **Não é vazamento.**
- **Pixel ID / Access Token:** vêm de **env var** (`META_PIXEL_ID`/`META_ACCESS_TOKEN`).
  **Nada hardcoded.** **Não reacusar.**
- **Obrigatórios repassados:** `action_source`, `event_time` (validado numérico), `event_id`
  (dedup), `event_source_url` (exceto CTWA), `value`/`currency` (custom_data repassado). ✅
- **Timeout 15s + tratamento de erro** (retorna status da Meta; 408 em AbortError). ✅
  Retry fica no caller `.03` (3x) — agora seguro por causa do lock in-flight.

---

## PENDENTE (precisa de ação humana / decisão)

- **Ativar `INTERNAL_SECRET`** nos 3 lados (proxy, .03, n8n) — precisa do Filipe gerar o
  segredo e **configurar o n8n** pra mandar o header. Enquanto não, roda fail-open.
- **Validar 1 evento real no Events Manager** pós-deploy (EMQ / parâmetros recebidos) —
  confirma o hash de ponta a ponta e que o `access_token` no body seguiu aceito.
- **`rateLimit` e o fallback de memória são por-instância** (serverless): o dedup real é o
  Redis (distribuído); o fallback de memória só vale dentro de 1 instância. Aceitável — só
  vira limitação se o Redis cair. Não corrigido nesta rodada (baixo risco).
