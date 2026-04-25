const { v5: uuidv5 } = require("uuid");
const NAMESPACE_TRANSPORTISTAS = "b8f9e3a1-7c2d-4f5e-9a1b-3c4d5e6f7a8b";

function generateTransportistaId(codigoTransportista) {
  return uuidv5(String(codigoTransportista).trim(), NAMESPACE_TRANSPORTISTAS);
}

module.exports = { generateTransportistaId };
