const { google } = require('googleapis');
const { Client } = require('@notionhq/client');
const { JWT } = require('google-auth-library');
const axios = require('axios');
const { Octokit } = require("@octokit/rest");

const GOOGLE_CREDS = require('./your-google-creds.json');
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const NOTION_TOKEN = process.env.NOTION_TOKEN;
const DATABASE_ID = process.env.DATABASE_ID;

const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const GITHUB_OWNER = process.env.GITHUB_OWNER;
const GITHUB_REPO = process.env.GITHUB_REPO;

const octokit = new Octokit({ auth: GITHUB_TOKEN });

const convertGDriveLink = (url) => {
  const fileIdMatch = url.match(/[-\w]{25,}/);
  if (!fileIdMatch) {
    throw new Error('Invalid Google Drive URL');
  }
  const fileId = fileIdMatch[0];
  return `https://drive.google.com/uc?export=view&id=${fileId}`;
};

async function uploadImageToGithub(url, filePath) {
  try {
    const response = await axios.get(url, { responseType: 'arraybuffer' });
    const content = Buffer.from(response.data, 'binary').toString('base64');

    const res = await octokit.repos.createOrUpdateFileContents({
      owner: GITHUB_OWNER,
      repo: GITHUB_REPO,
      path: filePath,
      message: `Add image ${filePath}`,
      content,
    });

    return res.data.content.download_url;
  } catch (err) {
    console.error("Error uploading image to GitHub:", err);
    throw err;
  }
}

const propertyTypes = {
  'picked?': 'checkbox',
  'time': 'date',
  'first_name': 'rich_text',
  'last_name': 'rich_text',
  'age': 'number',
  'height': 'number',
  'weight': 'number',
  'Размер одежды': 'rich_text',
  'Размер обуви': 'number',
  'headshotUrl': 'files',
  'bodyPhotoUrl': 'files',
  'Удобное время для съемки 6 апреля (можно выбрать несколько)': 'multi_select',
  'Номер телефона для связи': 'phone_number',
  'Ваш ник в Телеграм': 'rich_text',
  'Информация о правах субъекта персональных данных': 'rich_text',
  'Согласие субъекта персональных данных': 'rich_text',
  'Уточните, пожалуйста, ваш статус': 'select',
  'Укажите ваш Instagram': 'rich_text'
};

async function userExistsInNotion(notion, telegramHandle) {
  if (!telegramHandle) return false;

  const response = await notion.databases.query({
    database_id: DATABASE_ID,
    filter: {
      property: 'Ваш ник в Телеграм',
      rich_text: {
        equals: telegramHandle
      }
    }
  });

  return response.results.length > 0;
}

async function main() {
  const auth = new JWT({
    email: GOOGLE_CREDS.client_email,
    key: GOOGLE_CREDS.private_key,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: 'test!A1:Z',
  });

  const [headers, ...rows] = response.data.values;

  const notion = new Client({ auth: NOTION_TOKEN });

  for (const row of rows) {
    const properties = {};
    const embeddedImages = {};
    let telegramHandle = '';

    for (let i = 0; i < headers.length; i++) {
      const key = headers[i];
      const value = row[i];
      if (!value) continue;

      const propType = propertyTypes[key];
      if (!propType) continue;

      if (key === 'Ваш ник в Телеграм') {
        telegramHandle = value.trim();
      }

      switch (propType) {
        case 'checkbox':
          properties[key] = { checkbox: value.toLowerCase() === 'true' };
          break;

        case 'date':
          const [date, time] = value.split(' ');
          const [day, month, year] = date.split('.');
          properties[key] = {
            date: { start: `${year}-${month}-${day}T${time}` }
          };
          break;

        case 'files':
          try {
            const directUrl = convertGDriveLink(value);
            const fileExtension = '.jpg';
            const fileName = `${key}_${Date.now()}${fileExtension}`;
            const filePath = `images/${fileName}`;
            const githubUrl = await uploadImageToGithub(directUrl, filePath);

            embeddedImages[key] = githubUrl;

            properties[key] = {
              files: [{
                type: 'external',
                external: { url: githubUrl },
                name: key === 'headshotUrl' ? 'Headshot' : 'Body Photo'
              }]
            };
          } catch (error) {
            console.error(`Error processing ${key}:`, error);
          }
          break;

        case 'multi_select':
          properties[key] = {
            multi_select: value.split(',').map(t => ({ name: t.trim() }))
          };
          break;

        case 'select':
          properties[key] = { select: { name: value } };
          break;

        case 'rich_text':
          properties[key] = {
            rich_text: [{ text: { content: value } }]
          };
          break;

        case 'number':
          properties[key] = { number: parseFloat(value) };
          break;

        case 'title':
          properties[key] = {
            title: [{ text: { content: value } }]
          };
          break;

        case 'phone_number':
          properties[key] = { phone_number: value };
          break;
      }
    }

    if (!telegramHandle) {
      console.warn('Skipping row due to missing Telegram handle.');
      continue;
    }

    const alreadyExists = await userExistsInNotion(notion, telegramHandle);
    if (alreadyExists) {
      console.log(`Skipping existing user: ${telegramHandle}`);
      continue;
    }

    const createdPage = await notion.pages.create({
      parent: { database_id: DATABASE_ID },
      properties
    });

    const blocks = [];

    if (embeddedImages.headshotUrl) {
      blocks.push({
        object: 'block',
        type: 'image',
        image: {
          type: 'external',
          external: { url: embeddedImages.headshotUrl }
        }
      });
    }

    if (embeddedImages.bodyPhotoUrl) {
      blocks.push({
        object: 'block',
        type: 'image',
        image: {
          type: 'external',
          external: { url: embeddedImages.bodyPhotoUrl }
        }
      });
    }

    if (blocks.length > 0) {
      await notion.blocks.children.append({
        block_id: createdPage.id,
        children: blocks
      });
    }

    console.log(`Added new user: ${telegramHandle}`);
  }
}

main().catch(console.error);
