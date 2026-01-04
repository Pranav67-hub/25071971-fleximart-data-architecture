// FlexiMart MongoDB Operations (Part 2)
// Run inside Docker container:
// docker exec -it fleximart-mongo mongosh fleximart_nosql /import/mongodb_operations.js

const fs = require("fs");
const JSON_PATH = "/import/products_catalog.json";

// Use / create DB
db = db.getSiblingDB("fleximart_nosql");

// ------------------------------
// Operation 1: Load Data (1 mark)
// ------------------------------
print("\n=== Operation 1: Load Data ===");

if (!fs.existsSync(JSON_PATH)) {
  throw new Error("products_catalog.json not found at " + JSON_PATH);
}

const products = JSON.parse(fs.readFileSync(JSON_PATH, "utf8"));

db.products.drop();
const insertResult = db.products.insertMany(products);
print("Inserted documents:", insertResult.insertedIds ? Object.keys(insertResult.insertedIds).length : products.length);

// ------------------------------
// Operation 2: Basic Query (2 marks)
// Find all products in "Electronics" category with price < 50000
// Return only: name, price, stock
// ------------------------------
print("\n=== Operation 2: Basic Query (Electronics, price < 50000) ===");

db.products.find(
  { category: "Electronics", price: { $lt: 50000 } },
  { _id: 0, name: 1, price: 1, stock: 1 }
).forEach(doc => printjson(doc));

// ------------------------------
// Operation 3: Review Analysis (2 marks)
// Find all products that have average rating >= 4.0
// Use aggregation to calculate average from reviews array
// ------------------------------
print("\n=== Operation 3: Products with avg rating >= 4.0 ===");

db.products.aggregate([
  { $addFields: { avg_rating: { $avg: "$reviews.rating" } } },
  { $match: { avg_rating: { $gte: 4.0 } } },
  {
    $project: {
      _id: 0,
      product_id: 1,
      name: 1,
      category: 1,
      avg_rating: { $round: ["$avg_rating", 2] },
      review_count: { $size: "$reviews" }
    }
  },
  { $sort: { avg_rating: -1, review_count: -1 } }
]).forEach(doc => printjson(doc));

// ------------------------------
// Operation 4: Update Operation (2 marks)
// Add a new review to product "ELEC001"
// Review: {user: "U999", rating: 4, comment: "Good value", date: ISODate()}
// ------------------------------
print("\n=== Operation 4: Add review to ELEC001 ===");

const updateResult = db.products.updateOne(
  { product_id: "ELEC001" },
  {
    $push: {
      reviews: {
        user: "U999",
        rating: 4,
        comment: "Good value",
        date: new Date()
      }
    }
  }
);
printjson(updateResult);

// ------------------------------
// Operation 5: Complex Aggregation (3 marks)
// Calculate average price by category
// Return: category, avg_price, product_count
// Sort by avg_price descending
// ------------------------------
print("\n=== Operation 5: Avg price by category ===");

db.products.aggregate([
  {
    $group: {
      _id: "$category",
      avg_price: { $avg: "$price" },
      product_count: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      category: "$_id",
      avg_price: { $round: ["$avg_price", 2] },
      product_count: 1
    }
  },
  { $sort: { avg_price: -1 } }
]).forEach(doc => printjson(doc));

print("\n✅ All MongoDB operations completed.");
