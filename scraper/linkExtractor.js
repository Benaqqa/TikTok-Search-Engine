const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const { Kafka } = require('kafkajs');
const fs = require('fs').promises;
const path = require('path');

puppeteer.use(StealthPlugin());

const COOKIES_PATH = process.env.COOKIES_PATH || './cookies.json';
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'tiktok-video-links';
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const kafka = new Kafka({ clientId: 'tiktok-scraper', brokers: KAFKA_BROKERS });
const producer = kafka.producer();

function normalizeSameSite(sameSite) {
  if (!sameSite) return undefined;
  const s = String(sameSite).toLowerCase();
  if (s === 'no_restriction' || s === 'none') return 'None';
  if (s === 'lax') return 'Lax';
  if (s === 'strict') return 'Strict';
  return undefined;
}

function toPuppeteerCookie(c) {
  if (!c || !c.name) return null;
  const out = {
    name: c.name,
    value: c.value ?? '',
    domain: c.domain,
    path: c.path || '/',
    httpOnly: !!c.httpOnly,
    secure: !!c.secure,
  };
  const ss = normalizeSameSite(c.sameSite);
  if (ss) out.sameSite = ss;
  if (!c.session && typeof c.expirationDate === 'number') out.expires = c.expirationDate;
  return out;
}

async function loadCookies(page) {
  try {
    console.log(`Attempting to load cookies from ${COOKIES_PATH}...`);
    const raw = JSON.parse(await fs.readFile(COOKIES_PATH, 'utf-8'));
    if (!Array.isArray(raw)) throw new Error('cookies.json must be an array');
    const cookies = raw.map(toPuppeteerCookie).filter(Boolean);
    if (cookies.length === 0) { console.log('No valid cookies found in file'); return false; }
    await page.goto('https://www.tiktok.com/', { waitUntil: 'domcontentloaded' });
    await page.setCookie(...cookies);
    console.log(` Loaded ${cookies.length} cookies`);
    return true;
  } catch (err) {
    console.log(`No cookies loaded: ${err.message}`);
    return false;
  }
}

async function verifyCookies(page) {
  console.log('Verifying session validity...');
  await page.goto('https://www.tiktok.com', { waitUntil: 'networkidle2', timeout: 30000 });
  await delay(3000);
  const isLoggedIn = await page.evaluate(() =>
    !!document.querySelector('[data-e2e="profile-icon"]') ||
    !!document.querySelector('[data-e2e="user-avatar"]') ||
    !!document.querySelector('a[href*="/profile"]')
  );
  if (isLoggedIn) {
    const username = await page.evaluate(() => {
      const profileLink = document.querySelector('a[href*="/@"]');
      return profileLink ? profileLink.href.split('/@')[1] : 'unknown';
    });
    console.log(` Session valid — logged in as: @${username}`);
    return true;
  }
  console.log(' Cookies are invalid or expired');
  return false;
}

// ─── KEY CHANGE: producer is now passed in and each URL is streamed instantly ───
async function scrapeFromHomeFeed(page, producer, maxVideos = 100) {
  console.log(`\nNavigating to TikTok home feed...`);
  await page.goto('https://www.tiktok.com', { waitUntil: 'networkidle2', timeout: 60000 });

  try {
    await page.waitForSelector('[data-e2e="feed-video"], section[id^="media-card-"]', { timeout: 15000 });
    console.log('Home feed loaded');
  } catch (e) {
    console.log(' Timeout waiting for feed. Taking screenshot.');
    await page.screenshot({ path: 'debug_home_feed.png' });
  }

  const urls = new Set();
  let scrollAttempts = 0;
  const maxScrollAttempts = maxVideos * 2;
  console.log('Starting to scroll and stream video URLs...\n');

  while (urls.size < maxVideos && scrollAttempts < maxScrollAttempts) {
    const currentVideoData = await page.evaluate(() => {
      function isInViewportCenter(el) {
        const rect = el.getBoundingClientRect();
        const viewHeight = window.innerHeight || document.documentElement.clientHeight;
        return Math.abs(viewHeight / 2 - (rect.top + rect.height / 2)) < viewHeight / 3;
      }
      const sections = Array.from(document.querySelectorAll('[data-e2e="feed-video"], section[id^="media-card-"]'));
      const activeSection = sections.find(isInViewportCenter);
      if (!activeSection) return null;

      const authorLink = activeSection.parentElement.querySelector('a[href^="/@"]');
      const href = authorLink?.getAttribute('href');
      const username = href ? href.split('/@')[1]?.split('?')[0] : null;

      let videoId = null;
      const wrapper = activeSection.querySelector('[id^="xgwrapper-"]');
      if (wrapper) {
        const parts = wrapper.id.split('-');
        videoId = parts[parts.length - 1];
      }
      if (!videoId) {
        const video = activeSection.querySelector('video');
        if (video?.src) {
          const match = video.src.match(/item_id=(\d+)/);
          if (match) videoId = match[1];
        }
      }
      return username && videoId ? `https://www.tiktok.com/@${username}/video/${videoId}` : null;
    });

    if (currentVideoData && !urls.has(currentVideoData)) {
      urls.add(currentVideoData);

      // ── Stream instantly to Kafka as soon as URL is found ──
      try {
        await producer.send({
          topic: KAFKA_TOPIC,
          messages: [{
            key: currentVideoData.split('/video/')[1],
            value: JSON.stringify({
              url: currentVideoData,
              timestamp: new Date().toISOString(),
              source: 'tiktok-home-feed'
            })
          }]
        });
        process.stdout.write(`\rStreamed: ${urls.size}/${maxVideos} — ${currentVideoData}`);
      } catch (err) {
        console.error(`\n Failed to send to Kafka: ${err.message}`);
      }

      scrollAttempts = 0;
    } else {
      scrollAttempts++;
    }

    if (urls.size >= maxVideos) break;

    await page.keyboard.press('ArrowDown');
    await delay(1500 + Math.random() * 500);

    if (scrollAttempts > 5) {
      await page.mouse.wheel({ deltaY: 200 });
      await delay(1000);
    }
  }

  console.log(`\n\nFinished — streamed ${urls.size} URLs to Kafka`);
  return Array.from(urls);
}

(async () => {
  try {
    console.log(`Connecting to Kafka brokers: ${KAFKA_BROKERS.join(', ')}`);
    await producer.connect();
    console.log('Connected to Kafka');

    const browser = await puppeteer.launch({
      headless: 'new',
      args: [
        '--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled',
        '--disable-features=IsolateOrigins,site-per-process',
        '--window-size=1920,1080'
      ],
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined
    });

    const page = await browser.newPage();
    await page.setViewport({ width: 1920, height: 1080 });
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36');
    await page.setExtraHTTPHeaders({
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    });

    const cookiesLoaded = await loadCookies(page);
    if (!cookiesLoaded) {
      console.error('\n No valid cookies found.');
      await browser.close(); await producer.disconnect(); process.exit(1);
    }

    const sessionValid = await verifyCookies(page);
    if (!sessionValid) {
      console.error('\n Session cookies are invalid or expired.');
      await browser.close(); await producer.disconnect(); process.exit(1);
    }

    const linksDir = path.join(__dirname, 'Links');
    const metadataPath = path.join(linksDir, 'Metadata.txt');
    await fs.mkdir(linksDir, { recursive: true });

    const maxVideos = parseInt(process.env.MAX_VIDEOS) || 100;
    console.log(`\n=== Starting scrape from TikTok home feed (max ${maxVideos} videos) ===`);

    // ── Pass producer directly into scraper ──
    const urls = await scrapeFromHomeFeed(page, producer, maxVideos);

    if (urls.length > 0) {
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
      const outputPath = path.join(linksDir, `TikTokLinks_home_${timestamp}.txt`);
      await fs.writeFile(outputPath, urls.join('\n'));
      console.log(` Saved ${urls.length} URLs to: ${outputPath}`);
      await fs.writeFile(metadataPath, urls[urls.length - 1]);
      console.log(` Updated metadata with last link: ${urls[urls.length - 1]}`);
    } else {
      console.log('\n No videos collected.');
    }

    await browser.close();
    await producer.disconnect();
    console.log('\n Done!');
  } catch (error) {
    console.error('Error:', error);
    await producer.disconnect();
    process.exit(1);
  }
})();