// ========== WhatsApp Runtime Client Setup ==========
const { Client } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');

const waClient = new Client();

waClient.on('qr', (qr) => {
  qrcode.generate(qr, { small: true });
  console.log("📲 Scan this QR code with WhatsApp to activate the session.");
});

waClient.on('ready', () => {
  console.log('✅ WhatsApp client is ready and connected.');
});

waClient.initialize();

async function sendWhatsAppAlert(message) {
  const number = '8106764153'; // Replace with your WhatsApp number
  const chatId = `${number}@c.us`;

  try {
    await waClient.sendMessage(chatId, message);
    console.log("✅ WhatsApp alert sent to", number);
  } catch (error) {
    console.log("❌ Failed to send WhatsApp alert:", error.message);
  }
}