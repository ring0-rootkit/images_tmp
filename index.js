const { google } = require('googleapis');
const { JWT } = require('google-auth-library');
const { Client } = require('@notionhq/client');
const { Octokit } = require("@octokit/rest");
const axios = require('axios');
const pLimit = require('p-limit').default;

// Configuration
const GOOGLE_CREDS = require('./your-google-creds.json');
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const NOTION_TOKEN = process.env.NOTION_TOKEN;
const DATABASE_ID = process.env.DATABASE_ID;
const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const GITHUB_OWNER = process.env.GITHUB_OWNER;
const GITHUB_REPO = process.env.GITHUB_REPO;

// Initialize clients with rate limiting
const octokit = new Octokit({ auth: GITHUB_TOKEN });
const notion = new Client({ auth: NOTION_TOKEN });
const limit = pLimit(10); // Limit concurrent operations to prevent rate limiting

// Property mapping for Notion
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

// Helper function to convert Google Drive link to direct download URL
const convertGDriveLink = (url) => {
  if (!url) return null;
  try {
    const fileIdMatch = url.match(/[-\w]{25,}/);
    if (!fileIdMatch) return null;
    return `https://drive.google.com/uc?export=view&id=${fileIdMatch[0]}`;
  } catch (err) {
    console.warn('Failed to convert Google Drive URL:', url, err.message);
    return null;
  }
};

// Function to download image from URL
async function downloadImage(url) {
  try {
    const response = await axios.get(url, { responseType: 'arraybuffer', timeout: 10000 });
    return Buffer.from(response.data, 'binary');
  } catch (err) {
    console.warn('Failed to download image:', url, err.message);
    return null;
  }
}

//Function to upload images to GitHub in batches
async function uploadImagesToGithub(images) {
  if (!images.length) return {};

  const branch = 'master';
  const commitMessage = 'Add user submission images';
  const basePath = 'images/submissions/';

  try {
    // Get current reference
    const { data: refData } = await octokit.git.getRef({
      owner: GITHUB_OWNER,
      repo: GITHUB_REPO,
      ref: `heads/${branch}`,
    });

    const baseSha = refData.object.sha;

    // Get current commit
    const { data: baseCommit } = await octokit.git.getCommit({
      owner: GITHUB_OWNER,
      repo: GITHUB_REPO,
      commit_sha: baseSha,
    });

    // Create blobs for all images
    const blobs = await Promise.all(
      images.map(async (image) => {
        const { data } = await octokit.git.createBlob({
          owner: GITHUB_OWNER,
          repo: GITHUB_REPO,
          content: image.buffer.toString('base64'),
          encoding: 'base64'
        });
        return {
          path: `${basePath}${image.filename}`,
          mode: '100644',
          type: 'blob',
          sha: data.sha
        };
      })
    );

    // Create new tree with the images
    const { data: treeData } = await octokit.git.createTree({
      owner: GITHUB_OWNER,
      repo: GITHUB_REPO,
      base_tree: baseCommit.tree.sha,
      tree: blobs
    });

    // Create new commit
    const { data: commitData } = await octokit.git.createCommit({
      owner: GITHUB_OWNER,
      repo: GITHUB_REPO,
      message: commitMessage,
      tree: treeData.sha,
      parents: [baseSha],
    });

    // Update reference
    await octokit.git.updateRef({
      owner: GITHUB_OWNER,
      repo: GITHUB_REPO,
      ref: `heads/${branch}`,
      sha: commitData.sha,
    });

    // Create mapping of image keys to GitHub URLs
    const imageMap = {};
    images.forEach(image => {
      imageMap[image.key] = `https://raw.githubusercontent.com/${GITHUB_OWNER}/${GITHUB_REPO}/${branch}/${basePath}${image.filename}`;
    });

    return imageMap;
  } catch (err) {
    console.error('Error uploading images to GitHub:', err);
    throw err;
  }
}

// Check if user already exists in Notion
async function userExistsInNotion(telegramHandle) {
  if (!telegramHandle) return false;

  try {
    const response = await notion.databases.query({
      database_id: DATABASE_ID,
      filter: {
        property: 'Ваш ник в Телеграм',
        rich_text: { equals: telegramHandle }
      },
      page_size: 1
    });
    return response.results.length > 0;
  } catch (err) {
    console.error('Error checking user existence in Notion:', err);
    return false;
  }
}

// Process a batch of rows from Google Sheets
async function processBatch(batch, headers) {
  const imageUploadTasks = [];
  const notionCreationTasks = [];

  // First pass: collect all image URLs to download and prepare for upload
  for (const row of batch) {
    const rowData = {};
    for (let i = 0; i < headers.length; i++) {
      const key = headers[i];
      const value = row[i];
      if (value !== undefined && value !== null && value !== '') {
        rowData[key] = value;
      }
    }

    const telegramHandle = rowData['Ваш ник в Телеграм']?.trim();
    if (!telegramHandle) {
      console.warn('Skipping row without Telegram handle');
      continue;
    }

    // Prepare image upload tasks
    const imageFields = ['headshotUrl', 'bodyPhotoUrl'];
    for (const field of imageFields) {
      if (rowData[field]) {
        const directUrl = convertGDriveLink(rowData[field]);
        if (directUrl) {
          const filename = `${telegramHandle}_${field}_${Date.now()}.jpg`;
          const key = `${telegramHandle}_${field}`;

          imageUploadTasks.push(limit(async () => {
            const buffer = await downloadImage(directUrl);
            if (buffer) {
              return { buffer, filename, key };
            }
            return null;
          }));
        }
      }
    }
  }

  // Execute all image download tasks and filter out failures
  const imageUploads = (await Promise.all(imageUploadTasks)).filter(Boolean);

  // Upload all images in a single batch
  const imageUrlMap = imageUploads.length > 0
    ? await uploadImagesToGithub(imageUploads)
    : {};

  // Second pass: create Notion pages with the uploaded image URLs
  for (const row of batch) {
    const rowData = {};
    for (let i = 0; i < headers.length; i++) {
      const key = headers[i];
      const value = row[i];
      if (value !== undefined && value !== null && value !== '') {
        rowData[key] = value;
      }
    }

    const telegramHandle = rowData['Ваш ник в Телеграм']?.trim();
    if (!telegramHandle) continue;

    notionCreationTasks.push(limit(async () => {
      try {
        // Check if user already exists
        const exists = await userExistsInNotion(telegramHandle);
        if (exists) {
          console.log(`User already exists in Notion: ${telegramHandle}`);
          return { status: 'skipped', telegramHandle };
        }

        // Prepare Notion properties
        const properties = {};
        const imageBlocks = [];

        for (const [key, value] of Object.entries(rowData)) {
          if (!propertyTypes[key] || value === undefined || value === null || value === '') continue;

          switch (propertyTypes[key]) {
            case 'checkbox':
              properties[key] = { checkbox: value.toLowerCase() === 'true' };
              break;
            case 'date':
              try {
                // Handle empty or invalid values
                if (!value || typeof value !== 'string') {
                  console.warn(`Invalid date value for ${key}: ${value}`);
                  break;
                }

                // Split date and time parts
                const [datePart, timePart = '00:00'] = value.trim().split(' ');

                // Handle different date formats
                let day, month, year;
                if (datePart.includes('.')) {
                  [day, month, year] = datePart.split('.');
                } else if (datePart.includes('/')) {
                  [day, month, year] = datePart.split('/');
                } else if (datePart.includes('-')) {
                  [year, month, day] = datePart.split('-');
                } else {
                  throw new Error(`Unsupported date format: ${datePart}`);
                }

                // Pad single digits with leading zeros
                day = day.padStart(2, '0');
                month = month.padStart(2, '0');

                // Validate date components
                if (!day || !month || !year || isNaN(day) || isNaN(month) || isNaN(year)) {
                  throw new Error(`Invalid date components: ${day}.${month}.${year}`);
                }

                // Format time part
                const [hours = '00', minutes = '00', seconds = '00'] = timePart.split(':');
                const formattedTime = `${hours.padEnd(2, '0')}:${minutes.padEnd(2, '0')}`;

                properties[key] = {
                  date: {
                    start: `${year}-${month}-${day}T${formattedTime}`
                  }
                };
              } catch (err) {
                console.warn(`Invalid date format for ${key}: ${value}`, err.message);
              }
              break;
            case 'multi_select':
              properties[key] = {
                multi_select: value.split(',')
                  .map(v => v.trim())
                  .filter(v => v)
                  .map(v => ({ name: v }))
              };
              break;
            case 'select':
              properties[key] = { select: { name: value } };
              break;
            case 'number':
              const numValue = parseFloat(value);
              if (!isNaN(numValue)) {
                properties[key] = { number: numValue };
              }
              break;
            case 'phone_number':
              properties[key] = { phone_number: value.replace(/[^\d+]/g, '') };
              break;
            case 'rich_text':
              properties[key] = { rich_text: [{ text: { content: value.toString() } }] };
              break;
            case 'files':
              const imageKey = `${telegramHandle}_${key}`;
              const imageUrl = imageUrlMap[imageKey];
              if (imageUrl) {
                properties[key] = {
                  files: [{
                    type: 'external',
                    external: { url: imageUrl },
                    name: key === 'headshotUrl' ? 'Headshot' : 'Body Photo'
                  }]
                };
                imageBlocks.push({
                  object: 'block',
                  type: 'image',
                  image: {
                    type: 'external',
                    external: { url: imageUrl }
                  }
                });
              }
              break;
          }
        }

        // Create Notion page
        const page = await notion.pages.create({
          parent: { database_id: DATABASE_ID },
          properties
        });

        // Add image blocks if any
        if (imageBlocks.length > 0) {
          await notion.blocks.children.append({
            block_id: page.id,
            children: imageBlocks
          });
        }

        console.log(`✅ Created Notion page for: ${telegramHandle}`);
        return { status: 'success', telegramHandle };
      } catch (err) {
        console.error(`❌ Failed to create Notion page for ${telegramHandle}:`, err.message);
        return { status: 'failed', telegramHandle, error: err.message };
      }
    }));
  }

  // Wait for all Notion creations to complete
  const results = await Promise.all(notionCreationTasks);
  return results;
}

// Main function to fetch data from Google Sheets and process it
async function main() {
  try {
    console.log('Starting data import from Google Sheets to Notion...');

    // Authenticate with Google Sheets
    const auth = new JWT({
      email: GOOGLE_CREDS.client_email,
      key: GOOGLE_CREDS.private_key,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });

    const sheets = google.sheets({ version: 'v4', auth });

    // First, get the sheet metadata to determine the actual range
    const metadata = await sheets.spreadsheets.get({
      spreadsheetId: SPREADSHEET_ID,
      ranges: [],
      includeGridData: false
    });

    // Get the first sheet's properties
    const sheet = metadata.data.sheets[0];
    const lastRow = sheet.properties.gridProperties.rowCount;
    const lastColumn = sheet.properties.gridProperties.columnCount;
    const columnLetter = String.fromCharCode(64 + lastColumn); // Convert column number to letter

    // Fetch all data from Google Sheets with the correct range
    console.log('Fetching data from Google Sheets...');
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `A1:${columnLetter}${lastRow}`,
      majorDimension: 'ROWS'
    });

    if (!res.data.values || res.data.values.length === 0) {
      throw new Error('No data found in Google Sheets');
    }

    const [headers, ...rows] = res.data.values;
    const totalRows = rows.length;
    console.log(`Found ${totalRows} rows to process`);

    // Process in batches to avoid memory issues and rate limiting
    const batchSize = 20; // Adjust based on your needs
    let successCount = 0;
    let skippedCount = 0;
    let failedCount = 0;
    let processedCount = 0;

    for (let i = 0; i < totalRows; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      const batchNumber = Math.floor(i / batchSize) + 1;
      const totalBatches = Math.ceil(totalRows / batchSize);

      console.log(`Processing batch ${batchNumber} of ${totalBatches} (rows ${i + 1}-${Math.min(i + batchSize, totalRows)} of ${totalRows})...`);

      const results = await processBatch(batch, headers);
      processedCount += batch.length;

      results.forEach(result => {
        if (result.status === 'success') successCount++;
        else if (result.status === 'skipped') skippedCount++;
        else failedCount++;
      });

      // Add delay between batches to avoid rate limiting
      if (i + batchSize < totalRows) {
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }

    console.log('\n🎉 Processing complete!');
    console.log(`Total rows processed: ${processedCount}`);
    console.log(`✅ Success: ${successCount}`);
    console.log(`⏭ Skipped: ${skippedCount}`);
    console.log(`❌ Failed: ${failedCount}`);
  } catch (err) {
    console.error('❌ Error in main process:', err);
    process.exit(1);
  }
}

// Run the script
main();
