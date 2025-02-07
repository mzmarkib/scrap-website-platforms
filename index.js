require("dotenv").config();
const mysql = require("mysql2/promise");
const axios = require("axios");

const dbConfig = {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
};

const MAX_CONCURRENT_TASKS = parseInt(process.env.MAX_CONCURRENT_TASKS) || 5;
const FRAMEWORKS = process.env.FRAMEWORKS ? process.env.FRAMEWORKS.split(",") : [];
const HELP_DESKS = process.env.HELP_DESKS ? process.env.HELP_DESKS.split(",") : [];

async function createDbConnection() {
    try {
        const connection = await mysql.createConnection(dbConfig);
        console.log("✅ Database connected successfully!");
        return connection;
    } catch (error) {
        console.error("❌ Database connection failed:", error.message);
        process.exit(1);
    }
}

async function fetchRows(connection) {
    try {
        console.log("Fetching rows with status NULL...");
        const query = "SELECT id, url FROM domains WHERE status IS NULL LIMIT ?";
        console.log("Executing query:", query, "with value:", MAX_CONCURRENT_TASKS);
        const [rows] = await connection.query(query, [MAX_CONCURRENT_TASKS]);
        console.log("Fetched rows:", rows.length);
        return rows;
    } catch (error) {
        console.error("❌ Error fetching rows:", error.message);
        return [];
    }
}

function findKeywords(htmlString) {
    const lowerCaseHtml = htmlString.toLowerCase();
    function findMatches(list) {
        return [...new Set(list.filter(keyword => lowerCaseHtml.includes(keyword.toLowerCase())))];
    }
    function extractEmails(text) {
        return [...new Set(text.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/g) || [])];
    }
    function extractPageLinks(text, pageType) {
        const regex = new RegExp(`https?:\/\/[^\s"']*(?:${pageType})[^\s"']*`, 'gi');
        return [...new Set(text.match(regex) || [])];
    }

    const matchedFrameworks = findMatches(FRAMEWORKS);
    const matchedHelpDesks = findMatches(HELP_DESKS);
    const emails = extractEmails(htmlString);
    const contactPageLinks = extractPageLinks(htmlString, "contact");
    const faqPageLinks = extractPageLinks(htmlString, "faq");

    return {
        matched: matchedFrameworks.length > 0 || matchedHelpDesks.length > 0 || emails.length > 0 || contactPageLinks.length > 0 || faqPageLinks.length > 0,
        frameworks: matchedFrameworks,
        helpDesks: matchedHelpDesks,
        emails: emails,
        contactPageLinks: contactPageLinks,
        faqPageLinks: faqPageLinks
    };
}

async function processRow(connection, row) {
    try {
        console.log(`Processing row ID: ${row.id} - URL: ${row.url}`);

        // Mark as inProgress
        await connection.query("UPDATE domains SET status = 'inProgress' WHERE id = ?", [row.id]);

        // Fetch HTML content
        const response = await axios.get(row.url);
        const html = response.data;

        // Process the HTML content
        const processedData = findKeywords(html);

        // Update the database with processed data
        await connection.query(
            "UPDATE domains SET status = 'arkib', matched = ?, frameworks = ?, helpdesks = ?, emails = ?, contact_page_links = ?, faq_page_links = ?, data = ?, html = ? WHERE id = ?",
            [
                processedData.matched,
                JSON.stringify(processedData.frameworks),
                JSON.stringify(processedData.helpDesks),
                JSON.stringify(processedData.emails),
                JSON.stringify(processedData.contactPageLinks),
                JSON.stringify(processedData.faqPageLinks),
                JSON.stringify(processedData),
                html, // Store raw HTML if needed
                row.id
            ]
        );

        console.log(`✅ Processed row ID: ${row.id} successfully.`);
    } catch (error) {
        console.error(`❌ Error processing row ID: ${row.id}:`, error.message);

        // Mark as error
        await connection.query(
            "UPDATE domains SET status = 'errorArkib', data = ? WHERE id = ?",
            [error.message, row.id]
        );
    }
}


async function main() {
    const connection = await createDbConnection();
    while (true) {
        const rows = await fetchRows(connection);
        if (rows.length === 0) {
            console.log("No pending rows found. Retrying in 10 seconds...");
            await new Promise(resolve => setTimeout(resolve, 10000));
            continue;
        }
        await Promise.all(rows.map(row => processRow(connection, row)));
    }
}

main().catch(error => console.error("Unexpected error:", error.message));
