const qrcode = require('qrcode-terminal');
const { Client } = require('whatsapp-web.js');
const express = require('express');
const bodyParser = require('body-parser');

const app = express();
app.use(bodyParser.json());

const client = new Client();

client.on('qr', (qr) => {
  console.log('Scan the QR code below to authenticate:');
  qrcode.generate(qr, { small: true });
});

client.on('ready', () => {
  console.log('WhatsApp client is ready!');
});

client.on('error', (error) => {
  console.error('WhatsApp client error:', error);
});

app.post('/send-notification', async (req, res) => {
  const { number, message } = req.body;
  if (!number || !message) {
    return res.status(400).json({ error: 'Missing number or message' });
  }
  try {
    await client.sendMessage(`${number}@c.us`, message);
    res.status(200).json({ success: 'Message sent' });
  } catch (error) {
    console.error('Error sending WhatsApp message:', error);
    res.status(500).json({ error: 'Failed to send message' });
  }
});

client.initialize();

app.listen(3000, () => {
  console.log('Express server running on port 3000');
});