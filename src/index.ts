import { $, sleep } from "bun";
import Database from "bun:sqlite";
import * as cheerio from "cheerio";

const BASE_URL = "https://gdziestoja.pl";

interface URLInterface {
  type: string;
  url: string;
  created: string;
}

// 1. Open (or create) a SQLite DB and tables
const db = new Database("./data/raw.sqlite");
db.exec(`
  CREATE TABLE IF NOT EXISTS posts (
    post_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    title       TEXT,
    location    TEXT,
    description TEXT,
    date_added  DATETIME,
    author      TEXT,
    rating      REAL
  );

  CREATE TABLE IF NOT EXISTS comments (
    comment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id    INTEGER NOT NULL,
    parent_id  TEXT,
    text       TEXT,
    author     TEXT,
    score      TEXT,
    timestamp  TEXT,
    FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS urls (
    url_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    type       TEXT NOT NULL,
    url        TEXT UNIQUE NOT NULL,
    created    DATETIME
  );

  PRAGMA journal_mode=WAL;
`);

const insertPost = db.prepare(`
  INSERT OR REPLACE INTO posts
    (title, location, description, date_added, author, rating)
  VALUES (@title, @location, @description, @date_added, @author, @rating)
`);

const insertComment = db.prepare(`
  INSERT OR REPLACE INTO comments
    (post_id, parent_id, text, author, score, timestamp)
  VALUES (@post_id, @parent_id, @text, @author, @score, @timestamp)
`);

const insertUrl = db.prepare(`
  INSERT OR REPLACE INTO urls (type, url, created) VALUES (@type, @url, @created)
`);

const getAllVoivodeshipUrl = db.prepare(
  "SELECT url FROM urls WHERE type = 'voivodeship'"
);

const getAllCitiesUrl = db.prepare("SELECT url FROM urls WHERE type = 'city'");

async function getPageContent(url: string): Promise<string> {
  const res = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    },
  });

  const html = await res.text();

  return html;
}

// 4. Scrape function for one URL
async function startScrape() {
  try {
    const regionsURL: string[] = (
      getAllVoivodeshipUrl.all() as URLInterface[]
    ).map((r: URLInterface) => r.url);
    const citiesURL: string[] = (getAllCitiesUrl.all() as URLInterface[]).map(
      (r: URLInterface) => r.url
    );

    const homePage = await getPageContent(BASE_URL);
    const $ = cheerio.load(homePage);

    // Get regions
    if (regionsURL.length <= 0) {
      $(".list-group-item-prov a").each((_, el) => {
        const href = $(el).attr("href");
        if (href) {
          regionsURL.push(href);
          insertUrl.run("voivodeship", href, new Date().toISOString());
        }
      });
    }

    await sleep(Math.random() * 1000);

    // Get cities
    for (const regionURL of regionsURL) {
      const url = BASE_URL + regionURL;

      await sleep(Math.random() * 1000);

      const regionResponse = await getPageContent(url);
      const $region = cheerio.load(regionResponse);

      $region(".btn-city").each((_, el) => {
        const href = $region(el).attr("href");
        if (href) {
          if (!citiesURL.find((url) => href === url)) {
            citiesURL.push(href);
          }
          insertUrl.run("city", href, new Date().toISOString());
        }
      });
    }

    await sleep(Math.random() * 4000);

    // Get posts
    const postsURL: string[] = [];
    for (const cityURL of citiesURL) {
      const url = BASE_URL + cityURL;
      console.log(url);

      await sleep(Math.random() * 1000);

      const cityResponse = await getPageContent(url);
      const $city = cheerio.load(cityResponse);

      $city(".cellname a").each((_, el) => {
        const href = $city(el).attr("href");
        if (href && href.includes("/miejsce/")) {
          postsURL.push(href);
        }
      });
    }

    // Get post data and comments
    for (const postURL of postsURL) {
      const url = BASE_URL + postURL;

      await sleep(Math.random() * 2000);

      const postResponse = await getPageContent(url);
      const $post = cheerio.load(postResponse);

      const title = $post("article h4").text().trim();
      const description = $post(".place-description").text().trim();
      const rating = $post('[property="v:average"]')
        .text()
        .trim()
        .split(".")[0];
      const dateAdded = $post("time.text-light").attr("datetime");
      const dateAddedUTC = new Date(dateAdded!).toISOString();
      const author =
        $post("article.row").find(".userlink").text().trim() ||
        $post("article.row").find(".user").text().trim() ||
        "";

      const pagesCount = Math.max(
        1,
        $post("ul.pagination li.page-item").length - 1
      );

      // Get lat and lng from google map url
      const locationHref = $post("aside a").attr("href");
      const locationURL = new URL(locationHref!);
      const query = locationURL.searchParams.get("query");
      const [lat, lng] = query!.split(",");

      const postValues = {
        "@title": title,
        "@location": `${lat},${lng}`,
        "@description": description || "",
        "@date_added": dateAddedUTC || "",
        "@author": author || "Unknown",
        "@rating": rating ? parseFloat(rating) : 0,
      };

      const result = insertPost.run(postValues);

      // Get comments for each page
      for (let i = 1; i < pagesCount; i++) {
        const url = `${BASE_URL}${postURL.trim()}strona/${i}`;

        await sleep(Math.random() * 3000);

        const postResponse = await getPageContent(url);
        const $post = cheerio.load(postResponse);

        let lastParentCommentID: number | null = null;
        $post("article.cmt").each((_, el) => {
          let parentCommentID = null;

          // Check if comment have parent div
          const isDivParent = $post(el).parents("div.ml-4").length > 0;
          if (isDivParent) {
            parentCommentID = lastParentCommentID;
          }

          const author =
            $post(el).find(".userlink").text().trim() ||
            $post(el).find(".user").text().trim() ||
            "";

          const timestamp = new Date(
            $post(el).find("time").attr("datetime")!
          ).toISOString();

          let score = $post(el).find(".vc-sp").text().trim();
          if (!score || score.length <= 0) {
            score = "0";
          }

          const text = $post(el).find("p.cmt-c").text().trim();

          const commentValues = {
            "@post_id": result.lastInsertRowid,
            "@author": author,
            "@text": text,
            "@parent_id": parentCommentID || null,
            "@score": score,
            "@timestamp": timestamp,
          };

          const commentResult = insertComment.run(commentValues);

          if (!isDivParent) {
            lastParentCommentID = commentResult.lastInsertRowid as number;
          }
        });
      }
    }
  } catch (err) {
    console.error("Failed to scrape:", err);
    process.exit(1);
  }
}

await startScrape();

console.log("The end.");

db.close();
process.exit(0);
