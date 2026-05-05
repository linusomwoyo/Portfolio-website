const express = require('express');
const path = require('path');
const fs = require('fs').promises;
const nodemailer = require('nodemailer');
const dotenv = require('dotenv');

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;
const messagesFile = path.join(__dirname, 'messages.json');

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname)));

app.use((err, req, res, next) => {
  if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
    return res.status(400).json({ success: false, error: 'Invalid JSON payload.' });
  }
  return next(err);
});

async function saveMessage(message) {
  try {
    let existing = [];
    const data = await fs.readFile(messagesFile, 'utf8');
    try {
      existing = JSON.parse(data);
    } catch (parseError) {
      existing = [];
    }
    if (!Array.isArray(existing)) {
      existing = [];
    }
    existing.push(message);
    await fs.writeFile(messagesFile, JSON.stringify(existing, null, 2), 'utf8');
  } catch (error) {
    if (error.code === 'ENOENT') {
      await fs.writeFile(messagesFile, JSON.stringify([message], null, 2), 'utf8');
    } else {
      throw error;
    }
  }
}

async function sendEmail(message) {
  const { SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, EMAIL_TO, EMAIL_FROM } = process.env;

  if (!SMTP_HOST || !SMTP_PORT || !SMTP_USER || !SMTP_PASS || !EMAIL_TO || !EMAIL_FROM) {
    return false;
  }

  const transporter = nodemailer.createTransport({
    host: SMTP_HOST,
    port: Number(SMTP_PORT),
    secure: process.env.SMTP_SECURE === 'true',
    auth: {
      user: SMTP_USER,
      pass: SMTP_PASS,
    },
  });

  await transporter.sendMail({
    from: EMAIL_FROM,
    to: EMAIL_TO,
    subject: `New contact form message from ${message.name}`,
    text: `Name: ${message.name}\nEmail: ${message.email}\n\n${message.message}`,
    html: `<p><strong>Name:</strong> ${message.name}</p><p><strong>Email:</strong> ${message.email}</p><p>${message.message.replace(/\n/g, '<br>')}</p>`,
  });

  return true;
}

app.post('/api/contact', async (req, res) => {
  try {
    const { name, email, message } = req.body;

    if (!name || !email || !message) {
      return res.status(400).json({ success: false, error: 'Please fill in all fields.' });
    }

    const contactMessage = {
      name: String(name).trim(),
      email: String(email).trim(),
      message: String(message).trim(),
      submittedAt: new Date().toISOString(),
    };

    await saveMessage(contactMessage);
    const sent = await sendEmail(contactMessage);

    return res.json({ success: true, sent });
  } catch (error) {
    console.error('Contact form error:', error);
    return res.status(500).json({ success: false, error: 'Unable to submit your message right now.' });
  }
});

app.listen(PORT, () => {
  console.log(`Server is running at http://localhost:${PORT}`);
});
