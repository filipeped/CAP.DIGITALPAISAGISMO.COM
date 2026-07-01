// ✅ DIGITAL PAISAGISMO CAPI V9.2 - REDIS CACHE + TTL 24H
// V9.2: TTL aumentado para 24h (recomendado Meta)
// V9.1: Cache Redis para deduplicação distribuída
// - Upstash Redis para cache persistente entre instâncias
// - Fallback para Map() se Redis não configurado
// - Suporte a PII (email, telefone, nome) para integração com n8n
// - Deduplicação 24h, IPv6 inteligente
// - Tokens via variáveis de ambiente

import * as crypto from "crypto";
import * as zlib from "zlib";
import { Redis } from "@upstash/redis";

// Tipos para requisição e resposta (compatível com Express/Node.js)
interface UserData {
  external_id?: string;
  fbp?: string;
  fbc?: string;
  ct?: string;  // ✅ CORRETO: Meta CAPI usa 'ct' para city
  st?: string;  // ✅ CORRETO: Meta CAPI usa 'st' para state  
  zp?: string;  // ✅ CORRETO: Meta CAPI usa 'zp' para postal
  country?: string;  // ✅ Campo country
  // ✅ V8.8: ADICIONADO suporte a PII para n8n WhatsApp bot
  em?: string;  // email (será hasheado automaticamente)
  ph?: string;  // phone (será hasheado automaticamente)
  fn?: string;  // first name (será hasheado automaticamente)
  [key: string]: unknown;
}

interface EventData {
  event_id?: string;
  event_name?: string;
  event_time?: number | string;
  event_source_url?: string;
  action_source?: string;
  session_id?: string;
  user_data?: UserData;
  custom_data?: Record<string, unknown>;
  [key: string]: unknown;
}

interface ApiRequest {
  method?: string;
  body?: {
    data?: EventData[];
    [key: string]: unknown;
  };
  headers: Record<string, string | string[] | undefined>;
  socket?: {
    remoteAddress?: string;
  };
  cookies?: Record<string, string>;
}

interface ApiResponse {
  status(code: number): ApiResponse;
  json(data: unknown): void;
  end(): void;
  setHeader(name: string, value: string): void;
}

// ✅ SEGURANÇA: Tokens via variáveis de ambiente (configurar na Vercel)
const PIXEL_ID = (process.env.META_PIXEL_ID || "").trim();
const ACCESS_TOKEN = (process.env.META_ACCESS_TOKEN || "").trim();
const META_URL = `https://graph.facebook.com/v21.0/${PIXEL_ID}/events`;

// ✅ V9.3: Segredo compartilhado pra provar origem server-to-server (header x-internal-secret).
// Callers: .03 (api/_lib/meta-capi.js) e n8n. Fail-open: se a env NÃO estiver setada, o
// proxy aceita (não derruba produção) e loga aviso. Só ative depois que TODOS os callers
// (.03 e n8n) estiverem mandando o header — senão os eventos deles tomam 401. Ver AUDITORIA-CAPI.md.
const INTERNAL_SECRET = (process.env.INTERNAL_SECRET || "").trim();

// ⚠️ Validação de configuração
if (!PIXEL_ID || !ACCESS_TOKEN) {
  console.error("❌ ERRO CRÍTICO: META_PIXEL_ID e META_ACCESS_TOKEN devem estar configurados nas variáveis de ambiente!");
}

// ✅ SISTEMA DE DEDUPLICAÇÃO COM REDIS (V9.2)
const CACHE_TTL_SECONDS = 24 * 60 * 60; // 24 horas em segundos (recomendado Meta)
const REDIS_KEY_PREFIX = "capi:event:";

// Inicializar Redis (se configurado)
let redis: Redis | null = null;
const redisUrl = (process.env.UPSTASH_REDIS_REST_URL || "").trim();
const redisToken = (process.env.UPSTASH_REDIS_REST_TOKEN || "").trim();
const useRedis = !!(redisUrl && redisToken);

if (useRedis) {
  redis = new Redis({
    url: redisUrl,
    token: redisToken,
  });
  console.log("✅ Redis Upstash conectado para deduplicação distribuída");
} else {
  console.warn("⚠️ Redis não configurado - usando cache em memória (fallback)");
}

// Fallback: cache em memória (usado se Redis não estiver configurado)
const memoryCache = new Map<string, number>();
const MAX_MEMORY_CACHE = 50000;

// ✅ V9.3: Dedup em 2 fases (lock in-flight → confirma na entrega).
// Antes o event_id era gravado ANTES do POST pra Meta; se a Meta falhasse, o retry do
// caller era barrado como "duplicado" e o evento se PERDIA. Agora:
//  1. isDuplicateEvent() só ADQUIRE um lock curto (90s) via SET NX.
//  2. confirmDelivery() estende o TTL pra 24h SÓ após a Meta responder 2xx (dedup real).
//  3. releaseEvents() APAGA o lock se a Meta falhar → o retry passa a valer.
// Corridas de 2 requests idênticos: o 2º pega o lock existente e é tratado como dup —
// a própria Meta ainda deduplica nativamente por event_id, então nunca conta 2x.
const IN_FLIGHT_TTL_SECONDS = 90; // lock curto enquanto o evento está "em voo" pra Meta

async function isDuplicateEvent(eventId: string): Promise<boolean> {
  const cacheKey = `${REDIS_KEY_PREFIX}${eventId}`;

  // ✅ REDIS: Cache distribuído persistente
  if (redis) {
    try {
      // SET NX: só grava se ainda não existir. Lock in-flight de 90s (não 24h).
      // Retorna "OK" se adquiriu; null/undefined se já existia (em voo ou já entregue).
      const acquired = await redis.set(cacheKey, Date.now(), { nx: true, ex: IN_FLIGHT_TTL_SECONDS });
      if (acquired === null || acquired === undefined) {
        console.warn(`🚫 [REDIS] Evento duplicado/em-voo bloqueado: ${eventId}`);
        return true;
      }
      console.log(`🔒 [REDIS] Lock in-flight adquirido (90s): ${eventId}`);
      return false;

    } catch (error) {
      console.error("❌ Erro Redis, usando fallback em memória:", error);
      // Fallback para memória em caso de erro
    }
  }

  // ✅ FALLBACK: Cache em memória
  const now = Date.now();
  const ttlMs = CACHE_TTL_SECONDS * 1000;

  // Limpeza de expirados
  memoryCache.forEach((timestamp, id) => {
    if (now - timestamp > ttlMs) memoryCache.delete(id);
  });

  // Verificar duplicata
  if (memoryCache.has(eventId)) {
    console.warn(`🚫 [MEMORY] Evento duplicado bloqueado: ${eventId}`);
    return true;
  }

  // Controle de tamanho
  if (memoryCache.size >= MAX_MEMORY_CACHE) {
    const keys = Array.from(memoryCache.keys()).slice(0, 5000);
    keys.forEach(k => memoryCache.delete(k));
  }

  memoryCache.set(eventId, now);
  console.log(`✅ [MEMORY] Evento registrado (in-flight): ${eventId} (cache: ${memoryCache.size})`);
  return false;
}

// ✅ V9.3: Confirma a entrega — estende o lock pra 24h (dedup recomendado pela Meta).
// Chamado SÓ quando a Meta responde 2xx.
async function confirmDelivery(eventIds: string[]): Promise<void> {
  if (redis) {
    try {
      await Promise.all(
        eventIds.map((id) => redis!.set(`${REDIS_KEY_PREFIX}${id}`, Date.now(), { ex: CACHE_TTL_SECONDS }))
      );
      console.log(`✅ [REDIS] Entrega confirmada, dedup 24h: ${eventIds.length} evento(s)`);
    } catch (error) {
      console.error("❌ Erro Redis ao confirmar entrega (lock in-flight expira em 90s):", error);
    }
  }
  // Memória: o timestamp já está gravado com TTL de 24h — nada a fazer.
}

// ✅ V9.3: Libera os locks após FALHA na Meta, pra o retry do caller NÃO ser barrado.
async function releaseEvents(eventIds: string[]): Promise<void> {
  if (!eventIds.length) return;
  if (redis) {
    try {
      await Promise.all(eventIds.map((id) => redis!.del(`${REDIS_KEY_PREFIX}${id}`)));
      console.warn(`♻️ [REDIS] Locks liberados após falha (retry liberado): ${eventIds.length} evento(s)`);
    } catch (error) {
      console.error("❌ Erro Redis ao liberar locks (expiram sozinhos em 90s):", error);
    }
  }
  eventIds.forEach((id) => memoryCache.delete(id));
}

// ✅ REINTRODUZIDO: A função hashSHA256 é necessária como fallback para gerar event_id no servidor.
function hashSHA256(input: string): string {
  return crypto.createHash("sha256").update(input).digest("hex");
}

// ✅ V9.3: generateDeterministicValue REMOVIDA — o proxy não inventa mais valor de Lead
// (sujava a otimização por valor da Meta). Lead sem value agora vai sem value.

// ✅ IPv6 INTELIGENTE: Detecção e validação de IP com prioridade IPv6
function getClientIP(
  req: ApiRequest
): { ip: string; type: "IPv4" | "IPv6" | "unknown" } {
  const ipSources = [
    req.headers["cf-connecting-ip"],
    req.headers["x-real-ip"],
    req.headers["x-forwarded-for"],
    req.headers["x-client-ip"],
    req.headers["x-cluster-client-ip"],
    req.socket?.remoteAddress,
  ];

  const candidateIPs: string[] = [];
  ipSources.forEach((source) => {
    if (!source) return;
    if (typeof source === "string") {
      const ips = source.split(",").map((ip) => ip.trim());
      candidateIPs.push(...ips);
    }
  });

  function isValidIPv4(ip: string): boolean {
    const ipv4Regex =
      /^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/;
    return ipv4Regex.test(ip);
  }

  function isValidIPv6(ip: string): boolean {
    const cleanIP = ip.replace(/^\[|\]$/g, "");
    // ✅ REGEX IPv6 OTIMIZADA: Mais eficiente e simples
    try {
      // Validação básica de formato IPv6
      if (!/^[0-9a-fA-F:]+$/.test(cleanIP.replace(/\./g, ''))) return false;
      
      // Usar URL constructor para validação nativa (mais eficiente)
      new URL(`http://[${cleanIP}]`);
      return true;
    } catch {
      // Fallback para regex simplificada
      const ipv6Simple = /^([0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}$|^::1$|^::$/;
      return ipv6Simple.test(cleanIP);
    }
  }

  function isPrivateIP(ip: string): boolean {
    if (isValidIPv4(ip)) {
      const parts = ip.split(".").map(Number);
      // Validar se todas as partes são números válidos
      if (parts.some(part => isNaN(part) || part < 0 || part > 255)) {
        return false;
      }
      return (
        parts[0] === 10 ||
        (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) ||
        (parts[0] === 192 && parts[1] === 168) ||
        parts[0] === 127
      );
    }
    if (isValidIPv6(ip)) {
      const cleanIP = ip.replace(/^\[|\]$/g, "");
      return (
        cleanIP === "::1" ||
        cleanIP.startsWith("fe80:") ||
        cleanIP.startsWith("fc00:") ||
        cleanIP.startsWith("fd00:")
      );
    }
    return false;
  }

  const validIPv6: string[] = [];
  const validIPv4: string[] = [];

  candidateIPs.forEach((ip) => {
    if (isValidIPv6(ip) && !isPrivateIP(ip)) validIPv6.push(ip);
    else if (isValidIPv4(ip) && !isPrivateIP(ip)) validIPv4.push(ip);
  });

  // ✅ PRIORIDADE IPv6: Garantir que a Meta reconheça corretamente o IPv6
  if (validIPv6.length > 0) {
    const selectedIP = validIPv6[0];
    console.log("🌐 IPv6 detectado (prioridade para Meta CAPI):", selectedIP);
    return { ip: selectedIP, type: "IPv6" };
  }
  if (validIPv4.length > 0) {
    const selectedIP = validIPv4[0];
    console.log("🌐 IPv4 detectado (fallback):", selectedIP);
    return { ip: selectedIP, type: "IPv4" };
  }

  const fallbackIP = candidateIPs[0] || "unknown";
  console.warn("⚠️ IP não identificado, usando fallback:", fallbackIP);
  return { ip: fallbackIP, type: "unknown" };
}

// ✅ NOVA FUNÇÃO: Formatação otimizada de IP para Meta CAPI (consistente com frontend)
function formatIPForMeta(ip: string): string {
  // Detectar tipo de IP automaticamente
  const detectIPType = (ip: string): string => {
    if (!ip || ip === 'unknown') return 'unknown';
    
    // IPv6 contém ':'
    if (ip.includes(':')) {
      return 'IPv6';
    }
    
    // IPv4 contém apenas números e pontos
    if (/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(ip)) {
      return 'IPv4';
    }
    
    return 'unknown';
  };
  
  const ipType = detectIPType(ip);
  
  if (ipType === 'IPv6') {
    // Remove colchetes se presentes e garante formato limpo
    const cleanIP = ip.replace(/^\[|\]$/g, '');
    
    console.log('🌐 IPv6 formatado para Meta:', {
      original: ip,
      formatted: cleanIP,
      is_native_ipv6: true
    });
    
    return cleanIP;
  }
  
  if (ipType === 'IPv4') {
    // Para IPv4, a Meta recomenda conversão para IPv6-mapped
    // Formato IPv4-mapped IPv6: ::ffff:192.168.1.1
    const ipv6Mapped = `::ffff:${ip}`;
    console.log('🔄 IPv4 convertido para IPv6-mapped:', {
      original_ipv4: ip,
      ipv6_mapped: ipv6Mapped,
      reason: 'Meta prefere IPv6 sobre IPv4'
    });
    return ipv6Mapped;
  }
  
  return ip;
}

// ✅ CORREÇÃO CRÍTICA: Processamento FBC conforme documentação Meta oficial
function processFbc(fbc: string): string | null {
  if (!fbc || typeof fbc !== "string") {
    console.warn("⚠️ FBC inválido:", fbc);
    return null;
  }

  // ✅ CORREÇÃO META: Não usar trim() para preservar valor original
  // Meta documentação: "do not apply any modifications before using"
  
  // ✅ VALIDAÇÃO ANTI-MODIFICAÇÃO: Verificar se valor não foi alterado inadvertidamente
  const originalFbc = fbc;
  
  // ✅ CORREÇÃO CRÍTICA: Aceitar FBC já formatado (fb.subdomainIndex.timestamp.fbclid)
  // Documentação Meta: fb.[0-9]+.[0-9]{13}.[fbclid_value]
  // ✅ REGEX MAIS RIGOROSO: fbclid deve ter pelo menos 15 caracteres
  const fbcPattern = /^fb\.[0-9]+\.[0-9]{13}\.[A-Za-z0-9_-]{15,}$/;
  if (fbcPattern.test(fbc)) {
    console.log("✅ FBC válido (formato padrão Meta):", fbc.substring(0, 30) + '...');
    return fbc; // ✅ PRESERVA valor original sem modificações
  }

  // ✅ CORREÇÃO CRÍTICA: Envelope fbclid no formato Meta oficial
  // Meta documentação oficial: fb.1.timestamp.fbclid_value
  // ✅ VALIDAÇÃO MAIS RIGOROSA: fbclid deve ter pelo menos 15 caracteres
  const fbclidPattern = /^[A-Za-z0-9_-]{15,}$/; // Mais rigoroso: mínimo 15 chars
  
  // Se é um fbclid puro (sem prefixo fbclid=)
  if (fbclidPattern.test(fbc)) {
    // ✅ VERIFICAÇÃO ANTI-MODIFICAÇÃO: Garantir que não houve alteração de case
    if (fbc !== originalFbc) {
      console.error("❌ CRÍTICO: fbclid foi modificado durante processamento!");
      return null;
    }
    
    // ✅ CORREÇÃO CRÍTICA: Preservar timestamp original se possível
    // Meta documentação: "do not apply any modifications before using"
    const envelopedFbc = `fb.1.${Date.now()}.${fbc}`;
    console.log("✅ fbclid envelopado no formato Meta:", envelopedFbc.substring(0, 30) + '...');
    return envelopedFbc;
  }

  // ✅ CORREÇÃO: Se tem prefixo fbclid=, extrair o valor e envelopar corretamente
  if (fbc.startsWith("fbclid=")) {
    const fbclidValue = fbc.substring(7); // Remover "fbclid="
    if (fbclidValue.length >= 15 && /^[A-Za-z0-9_-]+$/.test(fbclidValue)) {
      const envelopedFbc = `fb.1.${Date.now()}.${fbclidValue}`;
      console.log("✅ fbclid extraído do prefixo e envelopado:", envelopedFbc.substring(0, 30) + '...');
      return envelopedFbc;
    }
    console.warn("⚠️ fbclid após remover prefixo é inválido:", fbclidValue);
    return null;
  }

  // ✅ CRÍTICO: Para formatos não reconhecidos, tentar envelope se parecer com fbclid
  // Meta documentação: "do not apply any modifications before using"
  if (fbc.length >= 10 && /^[A-Za-z0-9_-]+$/.test(fbc)) {
    const envelopedFbc = `fb.1.${Date.now()}.${fbc}`;
    console.log("✅ FBC formato não reconhecido - envelopando:", envelopedFbc);
    return envelopedFbc;
  }

  console.warn("⚠️ FBC inválido - não foi possível processar:", fbc);
  return null;
}

const RATE_LIMIT = 100; // Aumentado para suportar picos de tráfego
const rateLimitMap = new Map<string, number[]>();

function rateLimit(ip: string): boolean {
  const now = Date.now();
  const windowMs = 60000;
  if (!rateLimitMap.has(ip)) rateLimitMap.set(ip, []);
  const timestamps = (rateLimitMap.get(ip) || []).filter((t) => now - t < windowMs);
  if (timestamps.length >= RATE_LIMIT) return false;
  timestamps.push(now);
  rateLimitMap.set(ip, timestamps);
  if (rateLimitMap.size > 1000) {
    const oldest = rateLimitMap.keys().next();
    if (!oldest.done) rateLimitMap.delete(oldest.value);
  }
  return true;
}

export default async function handler(req: ApiRequest, res: ApiResponse) {
  const startTime = Date.now();

  const { ip, type: ipType } = getClientIP(req);
  const userAgent = (req.headers["user-agent"] as string) || "";
  const origin = (req.headers.origin as string) || "";

  const ALLOWED_ORIGINS = [
    "https://www.digitalpaisagismo.com",
    "https://digitalpaisagismo.com",
    "https://cap.digitalpaisagismo.com",
    "https://digitalpaisagismo-n8n.cloudfy.live",
    "https://consultoria.digitalpaisagismo.com",
    "https://www.consultoria.digitalpaisagismo.com",
    "https://cap.consultoria.digitalpaisagismo.com",
    "https://n8n.digitalpaisagismo.online",
    "https://n8n.jarviz.site",
    "https://gestao.digitalpaisagismo.com",
    "https://www.gestao.digitalpaisagismo.com",
    // ✅ SEGURANÇA: localhost removido em produção
    // Para dev local, adicione temporariamente ou use variável de ambiente
  ];

  res.setHeader(
    "Access-Control-Allow-Origin",
    ALLOWED_ORIGINS.includes(origin) ? origin : "https://www.digitalpaisagismo.com"
  );
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With");
  res.setHeader("Access-Control-Allow-Credentials", "true");
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "no-referrer");
  res.setHeader("X-Robots-Tag", "noindex, nofollow");
  res.setHeader("Strict-Transport-Security", "max-age=31536000; includeSubDomains");

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method Not Allowed" });
  if (!rateLimit(ip)) return res.status(429).json({ error: "Limite de requisições excedido", retry_after: 60 });

  // ✅ V9.3: Auth server-to-server fail-open. Sem INTERNAL_SECRET setado → aceita e avisa
  // (não derruba produção). Com a env setada → exige o header x-internal-secret batendo.
  if (INTERNAL_SECRET) {
    const got = String(req.headers["x-internal-secret"] || "").trim();
    if (got !== INTERNAL_SECRET) {
      console.warn("🚫 [AUTH] x-internal-secret inválido/ausente — evento recusado");
      return res.status(401).json({ error: "nao autorizado" });
    }
  } else {
    console.warn("⚠️ [AUTH] INTERNAL_SECRET não configurado — proxy fail-open (configure nos dois lados p/ ativar)");
  }

  // ✅ V9.3: event_ids cujo lock in-flight adquirimos nesta request. Se a Meta falhar
  // (erro ou exceção), liberamos esses locks pra o retry do caller não ser barrado.
  let acquiredEventIds: string[] = [];

  try {
    // ==================== PROCESSAMENTO FRONTEND ====================
    if (!req.body?.data || !Array.isArray(req.body.data)) {
      return res.status(400).json({ error: "Payload inválido - campo 'data' obrigatório" });
    }

    // 🛡️ FILTRO DE DEDUPLICAÇÃO MELHORADO: Verificar duplicatas antes do processamento
    const originalCount = req.body.data.length;
    // ✅ CORRIGIDO: Priorizar event_id do frontend para consistência Pixel/CAPI
    const eventsWithIds = req.body.data.map((event: EventData) => {
      if (!event.event_id) {
        // Gerar event_id determinístico apenas como fallback
        const eventName = event.event_name || "Lead";
        const eventTime = event.event_time && !isNaN(Number(event.event_time)) ? Math.floor(Number(event.event_time)) : Math.floor(Date.now() / 1000);
        const externalId = event.user_data?.external_id || "no_ext_id";
        const eventSourceUrl = event.event_source_url || origin || (req.headers.referer as string) || "https://www.digitalpaisagismo.com";
        const eventData = `${eventName}_${eventTime}_${externalId}_${eventSourceUrl}`;
        event.event_id = `evt_${hashSHA256(eventData).substring(0, 16)}`;
        console.warn("⚠️ Event_id gerado no servidor (fallback) - deve vir do frontend:", event.event_id);
      } else {
        console.log("✅ Event_id recebido do frontend (consistência Pixel/CAPI):", event.event_id);
      }
      return event;
    });
    
    // Segundo passo: filtrar duplicatas usando os event_ids (async para Redis)
    const deduplicationResults = await Promise.all(
      eventsWithIds.map(async (event: EventData) => {
        if (!event.event_id) return { event, isDuplicate: true };
        const isDuplicate = await isDuplicateEvent(event.event_id);
        return { event, isDuplicate };
      })
    );

    const filteredData = deduplicationResults
      .filter(({ isDuplicate }) => !isDuplicate)
      .map(({ event }) => event);

    // Locks in-flight que adquirimos agora → liberar se a Meta falhar.
    acquiredEventIds = filteredData
      .map((e) => e.event_id)
      .filter((id): id is string => typeof id === "string" && id.length > 0);

    const duplicatesBlocked = originalCount - filteredData.length;

    if (duplicatesBlocked > 0) {
      console.log(
        `🛡️ Deduplicação: ${duplicatesBlocked} eventos duplicados bloqueados de ${originalCount}`
      );
    }

    if (filteredData.length === 0) {
      return res.status(200).json({
        message: "Todos os eventos foram filtrados como duplicatas",
        duplicates_blocked: duplicatesBlocked,
        original_count: originalCount,
        cache_type: useRedis ? "redis" : "memory",
      });
    }

    // ✅ FORMATAÇÃO IPv6: Aplicar formatação otimizada para Meta CAPI
    const formattedIP = formatIPForMeta(ip);

    const enrichedData = filteredData.map((event: EventData) => {
      let externalId = event.user_data?.external_id || null;

      if (!externalId) {
        // ✅ CORRIGIDO: Usar EXATA lógica do DeduplicationEngine para consistência total
        let sessionId = event.session_id;
        if (!sessionId) {
          const anyReq = req as ApiRequest;
          if (anyReq.cookies && anyReq.cookies.session_id) {
            sessionId = anyReq.cookies.session_id;
          } else {
            // ✅ MESMA LÓGICA: Gerar sessionId idêntico ao DeduplicationEngine
            const timestamp = Date.now(); // Usar Date.now() como no frontend
            const randomStr = Math.random().toString(36).substr(2, 8); // Usar Math.random como no frontend
            sessionId = `sess_${timestamp}_${randomStr}`; // Mesmo formato: sess_timestamp_random
          }
        }
        
        // ✅ CONSISTÊNCIA TOTAL: Usar mesma lógica de geração do DeduplicationEngine
        const timestamp = Date.now();
        const randomPart = Math.random().toString(36).substr(2, 12);
        const baseId = `${timestamp}_${randomPart}_${sessionId}`;
        
        // ✅ Aplicar SHA256 idêntico ao DeduplicationEngine
        externalId = hashSHA256(baseId);
        console.log("⚠️ External_id gerado no servidor (fallback - IDÊNTICO ao DeduplicationEngine):", externalId);
      } else {
        // ✅ Hashear external_id se não estiver hasheado (protege PII do n8n/bots)
        if (externalId.length !== 64 || !/^[a-f0-9]{64}$/i.test(externalId)) {
          externalId = hashSHA256(externalId);
          console.log("🔐 External_id hasheado pelo CAPI (PII protegida):", externalId);
        } else {
          console.log("✅ External_id recebido já hasheado (SHA256):", externalId);
        }
      }

      const eventName = event.event_name || "Lead";
      const eventSourceUrl =
        event.event_source_url || origin || (req.headers.referer as string) || "https://www.digitalpaisagismo.com";
      const eventTime = event.event_time && !isNaN(Number(event.event_time)) ? Math.floor(Number(event.event_time)) : Math.floor(Date.now() / 1000);
      
      // ✅ Event_id já foi definido na etapa de deduplicação
      const eventId = event.event_id;
      console.log("✅ Event_id processado:", eventId);
      const actionSource = event.action_source || "website";

      const customData: Record<string, unknown> = { ...(event.custom_data || {}) };
      if (eventName === "PageView") {
        delete customData.value;
        delete customData.currency;
      }
      if (eventName === "Lead") {
        // ✅ V9.3: NÃO inventar valor. Antes o proxy gerava um valor fictício (R$10-100)
        // em Lead sem value, sujando a otimização por valor da Meta com dinheiro que não
        // existe. Agora: se o caller mandou value, mantém (e garante currency); se não
        // mandou, o Lead vai SEM value — mais honesto pra Meta. Ver AUDITORIA-CAPI.md.
        if (typeof customData.value === "undefined" || customData.value === null) {
          console.log(`ℹ️ Lead sem value — enviado sem valor (não inventamos mais). eventId: ${eventId?.substring(0, 16)}...`);
        } else {
          customData.currency = customData.currency || "BRL";
        }
      }

      // CTWA (business_messaging) NÃO aceita event_source_url/client_ip/client_user_agent (Meta rejeita).
      const ehBusinessMessaging = actionSource === "business_messaging";
      const userData: Record<string, unknown> = {
        ...(externalId && { external_id: externalId }),
        ...(ehBusinessMessaging ? {} : { client_ip_address: formattedIP, client_user_agent: userAgent }),
      };

      if (typeof event.user_data?.fbp === "string" && event.user_data.fbp.startsWith("fb.")) {
        // ✅ CORREÇÃO: FBP pode ter letras no timestamp (formato Meta flexível)
        const fbpPattern = /^fb\.[A-Za-z0-9]+\.[A-Za-z0-9]+\.[A-Za-z0-9_-]+$/;
        if (fbpPattern.test(event.user_data.fbp)) {
          userData.fbp = event.user_data.fbp;
          console.log("✅ FBP válido preservado:", event.user_data.fbp);
        } else {
          // ✅ CORREÇÃO: Preservar valor mesmo com formato não reconhecido
          userData.fbp = event.user_data.fbp;
          console.warn("⚠️ FBP formato não reconhecido, mas preservando:", event.user_data.fbp);
        }
      }

      if (event.user_data?.fbc) {
        const processedFbc = processFbc(event.user_data.fbc);
        if (processedFbc) {
          userData.fbc = processedFbc;
          console.log("✅ FBC processado e preservado:", processedFbc);
        } else {
          // ✅ CORREÇÃO: Preservar valor original mesmo quando processamento falha
          userData.fbc = event.user_data.fbc;
          console.warn("⚠️ FBC não processado, mas preservando valor original:", event.user_data.fbc);
        }
      }

      // ✅ CORREÇÃO CRÍTICA: Verificar se dados geográficos já estão hasheados (evitar double hash)
      // SHA256 sempre tem 64 caracteres hexadecimais - se já tem 64 chars, não re-hashear
      if (typeof event.user_data?.country === "string" && event.user_data.country.trim()) {
        const countryValue = event.user_data.country.trim();
        // Verificar se já está hasheado (64 caracteres hexadecimais = SHA256)
        if (countryValue.length === 64 && /^[a-f0-9]{64}$/i.test(countryValue)) {
          userData.country = countryValue;
          console.log("🌍 Country já hasheado (frontend):", countryValue.substring(0, 16) + '...');
        } else {
          // Fallback: aplicar hash se não estiver hasheado (sem modificar case)
          userData.country = hashSHA256(countryValue);
          console.log("🌍 Country hasheado (fallback API):", (userData.country as string).substring(0, 16) + '...');
        }
      }
      if (typeof event.user_data?.st === "string" && event.user_data.st.trim()) {
        const stateValue = event.user_data.st.trim();
        if (stateValue.length === 64 && /^[a-f0-9]{64}$/i.test(stateValue)) {
          userData.st = stateValue;
          console.log("🌍 State já hasheado (frontend):", stateValue.substring(0, 16) + '...');
        } else {
          userData.st = hashSHA256(stateValue);
          console.log("🌍 State hasheado (fallback API):", (userData.st as string).substring(0, 16) + '...');
        }
      }
      if (typeof event.user_data?.ct === "string" && event.user_data.ct.trim()) {
        const cityValue = event.user_data.ct.trim();
        if (cityValue.length === 64 && /^[a-f0-9]{64}$/i.test(cityValue)) {
          userData.ct = cityValue;
          console.log("🌍 City já hasheado (frontend):", cityValue.substring(0, 16) + '...');
        } else {
          userData.ct = hashSHA256(cityValue);
          console.log("🌍 City hasheado (fallback API):", (userData.ct as string).substring(0, 16) + '...');
        }
      }
      if (typeof event.user_data?.zp === "string" && event.user_data.zp.trim()) {
        const postalValue = event.user_data.zp.trim();
        if (postalValue.length === 64 && /^[a-f0-9]{64}$/i.test(postalValue)) {
          userData.zp = postalValue;
          console.log("🌍 Postal Code já hasheado (frontend):", postalValue.substring(0, 16) + '...');
        } else {
          userData.zp = hashSHA256(postalValue);
          console.log("🌍 Postal Code hasheado (fallback API):", (userData.zp as string).substring(0, 16) + '...');
        }
      }

      // ✅ V8.8: PROCESSAMENTO PII PARA N8N WHATSAPP BOT
      // Email - normaliza para lowercase antes de hashear
      if (typeof event.user_data?.em === "string" && event.user_data.em.trim()) {
        const emailValue = event.user_data.em.trim().toLowerCase();
        if (emailValue.length === 64 && /^[a-f0-9]{64}$/i.test(emailValue)) {
          userData.em = emailValue;
          console.log("📧 Email já hasheado (n8n):", emailValue.substring(0, 16) + '...');
        } else {
          userData.em = hashSHA256(emailValue);
          console.log("📧 Email hasheado (API):", (userData.em as string).substring(0, 16) + '...');
        }
      }

      // Telefone
      if (typeof event.user_data?.ph === "string" && event.user_data.ph.trim()) {
        const phoneRaw = event.user_data.ph.trim();
        // ✅ V9.3: testar "já hasheado" ANTES de remover não-dígitos. O SHA256 tem letras
        // a-f que o replace(/\D/) apagaria → antes isso causava DOUBLE-HASH de lixo.
        if (phoneRaw.length === 64 && /^[a-f0-9]{64}$/i.test(phoneRaw)) {
          userData.ph = phoneRaw.toLowerCase();
          console.log("📱 Telefone já hasheado (n8n):", (userData.ph as string).substring(0, 16) + '...');
        } else {
          let phoneDigits = phoneRaw.replace(/\D/g, '');
          // ✅ V9.3: rede de segurança do DDI Brasil (55). A Meta exige o código do país.
          // Nº nacional (DDD+numero) tem 10-11 dígitos; sem DDI o hash não bate. Prefixa 55.
          // (Com DDI já são 12-13 dígitos — não mexe.)
          if (phoneDigits.length === 10 || phoneDigits.length === 11) {
            phoneDigits = '55' + phoneDigits;
            console.log("📱 DDI 55 adicionado (telefone nacional sem código do país)");
          }
          if (phoneDigits.length > 0) {
            userData.ph = hashSHA256(phoneDigits);
            console.log("📱 Telefone hasheado (API):", (userData.ph as string).substring(0, 16) + '...');
          }
        }
      }

      // Nome - normaliza para lowercase antes de hashear
      if (typeof event.user_data?.fn === "string" && event.user_data.fn.trim()) {
        const nameValue = event.user_data.fn.trim().toLowerCase();
        if (nameValue.length === 64 && /^[a-f0-9]{64}$/i.test(nameValue)) {
          userData.fn = nameValue;
          console.log("👤 Nome já hasheado (n8n):", nameValue.substring(0, 16) + '...');
        } else {
          userData.fn = hashSHA256(nameValue);
          console.log("👤 Nome hasheado (API):", (userData.fn as string).substring(0, 16) + '...');
        }
      }

      // Sobrenome - normaliza para lowercase antes de hashear (igual ao fn)
      if (typeof event.user_data?.ln === "string" && event.user_data.ln.trim()) {
        const lastNameValue = event.user_data.ln.trim().toLowerCase();
        if (lastNameValue.length === 64 && /^[a-f0-9]{64}$/i.test(lastNameValue)) {
          userData.ln = lastNameValue;
          console.log("\u{1F464} Sobrenome ja hasheado (n8n):", lastNameValue.substring(0, 16) + '...');
        } else {
          userData.ln = hashSHA256(lastNameValue);
          console.log("\u{1F464} Sobrenome hasheado (API):", (userData.ln as string).substring(0, 16) + '...');
        }
      }

      // ctwa_clid - atribuicao Click-to-WhatsApp. Vai PLAIN (NAO hashear).
      if (typeof event.user_data?.ctwa_clid === "string" && event.user_data.ctwa_clid.trim()) {
        userData.ctwa_clid = event.user_data.ctwa_clid.trim();
        console.log("\u{1F517} ctwa_clid repassado (plain):", (userData.ctwa_clid as string).substring(0, 24) + '...');
      }

      // event_source_url só fora do fluxo CTWA (Meta proíbe em business_messaging).
      // messaging_channel só dentro do fluxo CTWA. Spread condicional preserva os tipos.
      const temMessagingChannel =
        typeof event.messaging_channel === "string" && event.messaging_channel.trim().length > 0;
      return {
        event_name: eventName,
        event_id: eventId,
        event_time: eventTime,
        action_source: actionSource,
        custom_data: customData,
        user_data: userData,
        ...(ehBusinessMessaging ? {} : { event_source_url: eventSourceUrl }),
        ...(temMessagingChannel
          ? { messaging_channel: (event.messaging_channel as string).trim() }
          : {}),
      };
    });

    const payload = { data: enrichedData };
    const jsonPayload = JSON.stringify(payload);
    const shouldCompress = Buffer.byteLength(jsonPayload) > 2048;
    const body = shouldCompress ? zlib.gzipSync(jsonPayload) : jsonPayload;
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      Connection: "keep-alive",
      "User-Agent": "DigitalPaisagismo-CAPI-Proxy/8.8",
      ...(shouldCompress ? { "Content-Encoding": "gzip" } : {}),
    };

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000); // Aumentado para 15s

    console.log("🔄 Enviando evento para Meta CAPI (V8.8 - COM PII):", {
      events: enrichedData.length,
      original_events: originalCount,
      duplicates_blocked: duplicatesBlocked,
      deduplication_rate: `${Math.round((duplicatesBlocked / originalCount) * 100)}%`,
      event_names: enrichedData.map((e) => e.event_name),
      event_ids: enrichedData.map((e) => e.event_id).slice(0, 3),
      ip_type: ip.includes(':') ? 'IPv6' : 'IPv4',
      client_ip_original: ip,
      client_ip_formatted: formattedIP,
      ipv6_conversion_applied: ip.includes(':') ? 'Native IPv6' : 'IPv4→IPv6-mapped',
      // ✅ V8.8: Log de PII (apenas indica presença, não mostra dados)
      has_pii: enrichedData.some((e) => e.user_data.em || e.user_data.ph || e.user_data.fn),
      pii_fields: {
        has_email: enrichedData.some((e) => e.user_data.em),
        has_phone: enrichedData.some((e) => e.user_data.ph),
        has_name: enrichedData.some((e) => e.user_data.fn),
      },
      external_ids_count: enrichedData.filter((e) => e.user_data.external_id).length,
      external_ids_from_frontend: enrichedData.filter(
        (e) => e.user_data.external_id && typeof e.user_data.external_id === 'string' && e.user_data.external_id.length === 64
      ).length,
      has_geo_data: enrichedData.some((e) => e.user_data.ct || e.user_data.st || e.user_data.zp),
      geo_locations: enrichedData
        .filter((e) => e.user_data.ct)
        .map((e) => `${e.user_data.ct}/${e.user_data.st}/${e.user_data.zp}`)
        .slice(0, 3),
      fbc_processed: enrichedData.filter((e) => e.user_data.fbc).length,
      cache_size: memoryCache.size,
      cache_ttl_hours: CACHE_TTL_SECONDS / 3600,
      cache_type: useRedis ? "redis" : "memory",
    });

    const response = await fetch(`${META_URL}?access_token=${ACCESS_TOKEN}`, {
      method: "POST",
      headers,
      body: body as BodyInit,
      signal: controller.signal,
    });

    clearTimeout(timeout);
    const data = await response.json() as Record<string, unknown>;
    const responseTime = Date.now() - startTime;

    if (!response.ok) {
      console.error("❌ Erro da Meta CAPI:", {
        status: response.status,
        data,
        events: enrichedData.length,
        ip_type: ip.includes(':') ? 'IPv6' : 'IPv4',
        duplicates_blocked: duplicatesBlocked,
      });

      // ✅ V9.3: Meta recusou → LIBERA os locks pra o retry do caller não ser barrado
      // como "duplicado" (senão o evento se perderia silenciosamente).
      await releaseEvents(acquiredEventIds);

      return res.status(response.status).json({
        error: "Erro da Meta",
        details: data,
        processing_time_ms: responseTime,
      });
    }

    // ✅ V9.3: Meta aceitou (2xx) → confirma a entrega, estendendo o dedup pra 24h.
    await confirmDelivery(acquiredEventIds);

    console.log("✅ Evento enviado com sucesso para Meta CAPI (V8.8):", {
      events_processed: enrichedData.length,
      duplicates_blocked: duplicatesBlocked,
      processing_time_ms: responseTime,
      compression_used: shouldCompress,
      ip_type: ip.includes(':') ? 'IPv6' : 'IPv4',
      external_ids_sent: enrichedData.filter((e) => e.user_data.external_id).length,
      pii_sent: {
        emails: enrichedData.filter((e) => e.user_data.em).length,
        phones: enrichedData.filter((e) => e.user_data.ph).length,
        names: enrichedData.filter((e) => e.user_data.fn).length,
      },
      sha256_format_count: enrichedData.filter(
        (e) => e.user_data.external_id && typeof e.user_data.external_id === 'string' && e.user_data.external_id.length === 64
      ).length,
      cache_size: memoryCache.size,
    });

    res.status(200).json({
      ...data,
      ip_info: { type: ip.includes(':') ? 'IPv6' : 'IPv4', address: ip },
      deduplication_info: {
        original_events: originalCount,
        processed_events: enrichedData.length,
        duplicates_blocked: duplicatesBlocked,
        cache_size: memoryCache.size,
      },
    });
  } catch (error: unknown) {
    console.error("❌ Erro no Proxy CAPI:", error);
    // ✅ V9.3: Timeout/erro de rede antes/depois do POST → libera os locks in-flight
    // pra o retry do caller poder reenviar (não fica preso como "duplicado").
    await releaseEvents(acquiredEventIds);
    if (error instanceof Error && error.name === "AbortError") {
      return res
        .status(408)
        .json({ error: "Timeout ao enviar evento para a Meta", timeout_ms: 15000 });
    }
    res.status(500).json({ error: "Erro interno no servidor CAPI." });
  }
}
