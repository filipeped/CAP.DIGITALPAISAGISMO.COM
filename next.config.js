module.exports = {
  // CORS é tratado dinamicamente em pages/api/events.ts (ALLOWED_ORIGINS),
  // por isso NÃO definimos Access-Control-* aqui — header global fixo
  // conflitava com a lista dinâmica e bloqueava origens válidas.
};
