'use strict';

/**
 * download-fonts.js
 *
 * Downloads NotoSansSC font files into backend/src/assets/fonts/ if they are
 * not already present.  Runs automatically via the "postinstall" npm hook so
 * every `npm install` keeps the fonts up-to-date.
 *
 * Font source: Google Fonts / noto-fonts GitHub repository (MIT / OFL-1.1)
 * NotoSans-Regular.ttf  – Latin + Vietnamese diacritics
 * NotoSansSC-Regular.ttf – Simplified Chinese + Latin + Vietnamese
 */

const fs   = require('fs');
const path = require('path');
const https = require('https');

const FONTS_DIR = path.join(__dirname, '../src/assets/fonts');

const FONTS = [
  {
    name: 'NotoSansSC-Regular.otf',
    url:  'https://raw.githubusercontent.com/notofonts/noto-cjk/main/Sans/SubsetOTF/SC/NotoSansSC-Regular.otf',
    // Fallback: NotoSans TTF (Latin + Vietnamese, no CJK) — still better than Helvetica for Vietnamese
    fallbackUrl: 'https://raw.githubusercontent.com/googlefonts/noto-fonts/main/hinted/ttf/NotoSans/NotoSans-Regular.ttf',
    fallbackName: 'NotoSansSC-Regular.ttf',
  },
  {
    name: 'NotoSansSC-Bold.otf',
    url:  'https://raw.githubusercontent.com/notofonts/noto-cjk/main/Sans/SubsetOTF/SC/NotoSansSC-Bold.otf',
    fallbackUrl: 'https://raw.githubusercontent.com/googlefonts/noto-fonts/main/hinted/ttf/NotoSans/NotoSans-Bold.ttf',
    fallbackName: 'NotoSansSC-Bold.ttf',
  },
];

function download(url, destPath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(destPath);
    const request = https.get(url, (response) => {
      if (response.statusCode === 301 || response.statusCode === 302) {
        file.close();
        fs.unlink(destPath, () => {});
        download(response.headers.location, destPath).then(resolve).catch(reject);
        return;
      }
      if (response.statusCode !== 200) {
        file.close();
        fs.unlink(destPath, () => {});
        reject(new Error(`HTTP ${response.statusCode} for ${url}`));
        return;
      }
      response.pipe(file);
      file.on('finish', () => file.close(resolve));
    });
    request.on('error', (err) => {
      file.close();
      fs.unlink(destPath, () => {});
      reject(err);
    });
    file.on('error', (err) => {
      fs.unlink(destPath, () => {});
      reject(err);
    });
  });
}

async function downloadFont(font) {
  const destPath = path.join(FONTS_DIR, font.name);

  if (fs.existsSync(destPath)) {
    console.log(`[fonts] ${font.name} already exists, skipping.`);
    return;
  }

  console.log(`[fonts] Downloading ${font.name}...`);
  try {
    await download(font.url, destPath);
    console.log(`[fonts] ${font.name} downloaded successfully.`);
  } catch (err) {
    console.warn(`[fonts] Primary URL failed (${err.message}), trying fallback...`);
    // When using the fallback, the file may have a different extension (.ttf vs .otf)
    const fallbackDest = font.fallbackName
      ? path.join(FONTS_DIR, font.fallbackName)
      : destPath;
    try {
      await download(font.fallbackUrl, fallbackDest);
      console.log(`[fonts] ${path.basename(fallbackDest)} downloaded from fallback successfully.`);
    } catch (fallbackErr) {
      console.warn(`[fonts] Could not download ${font.name}: ${fallbackErr.message}`);
      console.warn('[fonts] PDF will fall back to Helvetica (Latin-only). Chinese characters may not render correctly.');
    }
  }
}

async function main() {
  if (!fs.existsSync(FONTS_DIR)) {
    fs.mkdirSync(FONTS_DIR, { recursive: true });
  }

  for (const font of FONTS) {
    await downloadFont(font);
  }
}

main().catch((err) => {
  console.error('[fonts] Unexpected error:', err.message);
  // Do not exit with error code — font download failure is non-fatal
});
