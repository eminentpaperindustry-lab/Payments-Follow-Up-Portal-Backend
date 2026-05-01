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
  exposedHeaders: ['Cache-Control', 'Last-Modified']
}))

app.use(express.json())

// Global cache middleware for API responses
app.use((req, res, next) => {
  // Skip cache for update operations
  if (req.method === 'POST' || req.method === 'PUT' || req.method === 'DELETE') {
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
      // Cache for 30 seconds for dynamic data, 5 minutes for static data
      const ttl = req.url.includes('/parties') ? 300000 : 30000
      cache.put(key, body, ttl)
      res.sendResponse(body)
    }
    next()
  }
})

app.use("/api/payments", paymentRoutes)

app.listen(process.env.PORT, () => {
  console.log("Server running on port", process.env.PORT)
})