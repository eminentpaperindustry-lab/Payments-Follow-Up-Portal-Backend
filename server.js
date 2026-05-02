require("dotenv").config()
const express = require("express")
const cors = require("cors")
const compression = require("compression")
const cache = require("memory-cache")

const paymentRoutes = require("./routes/payments")

const app = express()

// Enable compression for faster responses
app.use(compression())

// Configure CORS with caching headers
app.use(cors({
  exposedHeaders: ['Cache-Control', 'Last-Modified', 'X-Cache']
}))

app.use(express.json({ limit: '10mb' }))
app.use(express.urlencoded({ extended: true, limit: '10mb' }))

// Global cache middleware for API responses with better cache control
app.use((req, res, next) => {
  // Skip cache for update operations
  if (req.method === 'POST' || req.method === 'PUT' || req.method === 'DELETE') {
    // Clear cache for related endpoints
    if (req.url.includes('/update-followup')) {
      const keysToClear = cache.keys().filter(key => 
        key.includes('filtered_') || key.includes('__express__/api/payments')
      )
      keysToClear.forEach(key => cache.del(key))
      console.log(`🗑️ Cleared ${keysToClear.length} cache keys due to update`)
    }
    return next()
  }
  
  // Check for cache bypass header
  if (req.query.skipCache === 'true') {
    console.log("🔄 Cache bypass requested")
    return next()
  }
  
  const key = `__express__${req.originalUrl || req.url}`
  const cachedBody = cache.get(key)
  
  if (cachedBody) {
    res.setHeader('X-Cache', 'HIT')
    return res.send(cachedBody)
  } else {
    res.sendResponse = res.send
    res.send = (body) => {
      // Cache for 15 seconds for dynamic data, 5 minutes for static data
      let ttl = 15000 // Default 15 seconds
      if (req.url.includes('/parties')) {
        ttl = 300000 // 5 minutes for parties
      } else if (req.url.includes('/consignees')) {
        ttl = 120000 // 2 minutes for consignees
      }
      cache.put(key, body, ttl)
      res.sendResponse(body)
    }
    next()
  }
})

// Health check endpoint
app.get("/health", (req, res) => {
  res.json({ status: "OK", timestamp: new Date().toISOString() })
})

// Clear all cache endpoint
app.post("/clear-cache", (req, res) => {
  cache.clear()
  console.log("🗑️ All cache cleared")
  res.json({ success: true, message: "All cache cleared" })
})

app.use("/api/payments", paymentRoutes)

const PORT = process.env.PORT || 5003
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`)
  console.log(`📊 Environment: ${process.env.NODE_ENV || 'development'}`)
})