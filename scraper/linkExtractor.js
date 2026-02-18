const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs').promises;

puppeteer.use(StealthPlugin());

const COOKIES_PATH = process.env.COOKIES_PATH || './cookies.json';
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

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
  if (!c.session && typeof c.expirationDate === 'number') {
    out.expires = c.expirationDate;
  }
  return out;
}

async function loadCookies(page) {
  try {
    console.log(`Attempting to load cookies from ${COOKIES_PATH}...`);
    const raw = JSON.parse(await fs.readFile(COOKIES_PATH, 'utf-8'));
    if (!Array.isArray(raw)) {
      throw new Error('cookies.json must be an array');
    }
    const cookies = raw.map(toPuppeteerCookie).filter(Boolean);
    if (cookies.length === 0) {
      console.log('No valid cookies found in file');
      return false;
    }
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
  await page.goto('https://www.tiktok.com', {
    waitUntil: 'networkidle2',
    timeout: 30000
  });
  await delay(3000);
  
  const isLoggedIn = await page.evaluate(() => {
    return !!document.querySelector('[data-e2e="profile-icon"]') ||
           !!document.querySelector('[data-e2e="user-avatar"]') ||
           !!document.querySelector('a[href*="/profile"]');
  });
  
  if (isLoggedIn) {
    console.log(' Session is valid, logged in successfully!');
    const username = await page.evaluate(() => {
      const profileLink = document.querySelector('a[href*="/@"]');
      return profileLink ? profileLink.href.split('/@')[1] : 'unknown';
    });
    console.log(` Logged in as: @${username}`);
    return true;
  } else {
    console.log(' Cookies are invalid or expired');
    return false;
  }
}

async function scrapeFromHomeFeed(page, maxVideos = 100) {
  console.log(`\nNavigating to TikTok home feed...`);
  
  await page.goto('https://www.tiktok.com', { 
    waitUntil: 'networkidle2', 
    timeout: 60000 
  });
  
  // Wait for the feed to load (targeting the section tag you shared)
  try {
    await page.waitForSelector('[data-e2e="feed-video"], section[id^="media-card-"]', { 
      timeout: 15000 
    });
    console.log('✓ Home feed loaded');
  } catch (e) {
    console.log("⚠️ Timeout waiting for feed. Taking screenshot.");
    await page.screenshot({ path: 'debug_home_feed.png' });
  }

  let urls = new Set();
  let scrollAttempts = 0;
  const maxScrollAttempts = maxVideos * 2;

  console.log('Starting to scroll and collect video URLs...\n');

  while (urls.size < maxVideos && scrollAttempts < maxScrollAttempts) {
    
    // Find the currently visible video ID and Username
    const currentVideoData = await page.evaluate(() => {
      // Helper: Check if element is in viewport center
      function isInViewportCenter(el) {
        const rect = el.getBoundingClientRect();
        const viewHeight = window.innerHeight || document.documentElement.clientHeight;
        const center = viewHeight / 2;
        const elCenter = rect.top + (rect.height / 2);
        // Be strict: element must be close to center
        return Math.abs(center - elCenter) < (viewHeight / 3);
      }

      // Find all video sections
      const sections = Array.from(document.querySelectorAll('[data-e2e="feed-video"], section[id^="media-card-"]'));
      const activeSection = sections.find(isInViewportCenter);

      if (activeSection) {
        // 1. Try to find username
        const authorLink = activeSection.parentElement.querySelector('a[href^="/@"]');
        let username = null;
        if (authorLink) {
             // Extract "idabou_01" from "/@idabou_01"
             const href = authorLink.getAttribute('href');
             if (href) username = href.split('/@')[1]?.split('?')[0];
        }

        // 2. Try to find video ID
        let videoId = null;
        
        // Method A: Check the wrapper ID (xgwrapper-0-7604144177886137622)
        const wrapper = activeSection.querySelector('[id^="xgwrapper-"]');
        if (wrapper) {
            const parts = wrapper.id.split('-');
            // usually the last part is the ID
            videoId = parts[parts.length - 1]; 
        }

        // Method B: Check the video src attribute
        if (!videoId) {
            const video = activeSection.querySelector('video');
            if (video && video.src) {
                // src="...&item_id=7604144177886137622&..."
                const match = video.src.match(/item_id=(\d+)/);
                if (match) videoId = match[1];
            }
        }

        // Return the constructed URL if we have both pieces
        if (username && videoId) {
            return `https://www.tiktok.com/@${username}/video/${videoId}`;
        }
      }
      
      return null;
    });

    if (currentVideoData) {
        if (!urls.has(currentVideoData)) {
            urls.add(currentVideoData);
            process.stdout.write(`\rCollected: ${urls.size}/${maxVideos} videos`);
            scrollAttempts = 0;
        } else {
            scrollAttempts++;
        }
    } else {
        scrollAttempts++;
    }

    if (urls.size >= maxVideos) break;

    // Scroll Action (ArrowDown for Feed Snap)
    await page.keyboard.press('ArrowDown');
    await delay(1500 + Math.random() * 500);

    // Stuck protection
    if (scrollAttempts > 5) {
        // Fallback: Try a small mouse wheel scroll to unstuck
        await page.mouse.wheel({ deltaY: 200 });
        await delay(1000);
    }
  }
  
  console.log(`\n\n✓ Finished collecting ${urls.size} URLs`);
  return Array.from(urls);
}



(async () => {
  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
      '--disable-features=IsolateOrigins,site-per-process',
      '--window-size=1920,1080'
    ],
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined
  });
  
  const page = await browser.newPage();
  
  await page.setViewport({ width: 1920, height: 1080 });
  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
  );
  await page.setExtraHTTPHeaders({
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
  });
  
  // Load cookies
  const cookiesLoaded = await loadCookies(page);
  if (!cookiesLoaded) {
    console.error('\n No valid cookies found. Please export cookies from your browser.');
    await browser.close();
    process.exit(1);
  }
  
  // Verify cookies
  const sessionValid = await verifyCookies(page);
  if (!sessionValid) {
    console.error('\n Session cookies are invalid or expired.');
    await browser.close();
    process.exit(1);
  }
  
  // Setup paths
  const path = require('path');
  const linksDir = path.join(__dirname, 'Links');
  const metadataPath = path.join(linksDir, 'Metadata.txt');
  
  // Ensure Links directory exists
  try {
    await fs.mkdir(linksDir, { recursive: true });
  } catch (err) {
    if (err.code !== 'EEXIST') throw err;
  }
  
  // Scrape from home feed
  const maxVideos = parseInt(process.env.MAX_VIDEOS) || 100;
  console.log(`\n=== Starting scrape from TikTok home feed (max ${maxVideos} videos) ===`);
  
  const urls = await scrapeFromHomeFeed(page, maxVideos);
  
  if (urls.length > 0) {
    // Save results
    const now = new Date();
    const timestamp = now.toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const filename = `TikTokLinks_home_${timestamp}.txt`;
    const outputPath = path.join(linksDir, filename);
    
    await fs.writeFile(outputPath, urls.join('\n'));
    console.log(`\n Saved ${urls.length} URLs to: ${outputPath}`);
    
    // Update Metadata.txt with the LAST link
    const lastLink = urls[urls.length - 1];
    await fs.writeFile(metadataPath, lastLink);
    console.log(` Updated metadata with last link: ${lastLink}`);
  } else {
    console.log('\n No videos collected.');
  }
  
  await browser.close();
  console.log('\n Done!');
})();
