const { google } = require('googleapis');
const { Client } = require('@notionhq/client');
const { JWT } = require('google-auth-library');
const fs = require('fs');
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

// Function to convert Google Drive link to direct URL
const convertGDriveLink = (url) => {
  // Assumes URL format: https://drive.google.com/file/d/FILE_ID/view?usp=sharing
  const fileIdMatch = url.match(/[-\w]{25,}/);
  if (!fileIdMatch) {
    throw new Error('Invalid Google Drive URL');
  }
  const fileId = fileIdMatch[0];
  return `https://drive.google.com/uc?export=view&id=${fileId}`;
};

// Function to download an image from a URL and upload it to GitHub
async function uploadImageToGithub(url, filePath) {
  try {
    // Download image data as an arraybuffer
    const response = await axios.get(url, { responseType: 'arraybuffer' });
    const content = Buffer.from(response.data, 'binary').toString('base64');

    // Upload the file to GitHub. This will create or update the file.
    const res = await octokit.repos.createOrUpdateFileContents({
      owner: GITHUB_OWNER,
      repo: GITHUB_REPO,
      path: filePath,
      message: `Add image ${filePath}`,
      content,
    });
    // Return the download URL from GitHub (raw file URL)
    return res.data.content.download_url;
  } catch (err) {
    console.error("Error uploading image to GitHub:", err);
    throw err;
  }
}

// Property types for Notion
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

async function main() {
  // Initialize Google Sheets API
  const auth = new JWT({
    email: GOOGLE_CREDS.client_email,
    key: GOOGLE_CREDS.private_key,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });

  // Get data from the spreadsheet
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: 'test!A1:Z',
  });

  const [headers, ...rows] = response.data.values;

  // Initialize Notion client
  const notion = new Client({ auth: NOTION_TOKEN });

  // Process each row from the spreadsheet
  for (const row of rows) {
    const properties = {};

    for (let i = 0; i < headers.length; i++) {
      const key = headers[i];
      const value = row[i];
      if (!value) continue;

      const propType = propertyTypes[key];
      if (!propType) continue;

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
            // Convert the Google Drive URL to a direct URL
            const directUrl = convertGDriveLink(value);
            // Create a unique file path. You can adjust the naming as needed.
            const fileExtension = '.jpg'; // Change as needed based on your file type
            const fileName = `${key}_${Date.now()}${fileExtension}`;
            const filePath = `images/${fileName}`;
            // Upload image to GitHub and retrieve the raw URL
            const githubUrl = await uploadImageToGithub(directUrl, filePath);
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

    // Create a page in Notion with the properties
    await notion.pages.create({
      parent: { database_id: DATABASE_ID },
      properties
    });
  }
}

main().catch(console.error);
