const { Client } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const express = require('express');
const bodyParser = require('body-parser');

const app = express();
app.use(bodyParser.json());

const waClient = new Client();

waClient.on('qr', (qr) => {
  qrcode.generate(qr, { small: true });
  console.log("📲 Scan the QR code using WhatsApp.");
});

waClient.on('ready', () => {
  console.log('✅ WhatsApp client is ready and connected.');
});

app.post('/send', async (req, res) => {
  const { number, message } = req.body;
  const chatId = number.includes('@c.us') ? number : `${number}@c.us`;

  try {
    await waClient.sendMessage(chatId, message);
    console.log(`✅ Sent WhatsApp message to ${number}`);
    res.status(200).send("Message sent");
  } catch (err) {
    console.error("❌ Failed to send message:", err.message);
    res.status(500).send("Error sending message");
  }
});

waClient.initialize();
app.listen(3000, () => console.log('📡 WhatsApp notifier server running on http://localhost:3000'));
