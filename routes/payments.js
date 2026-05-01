const router = require("express").Router()
const getSheets = require("../googleSheet")
const NodeCache = require("node-cache")

// Initialize cache with different TTLs
const cache = new NodeCache({ stdTTL: 300, checkperiod: 120 })
const dataCache = new NodeCache({ stdTTL: 30, checkperiod: 60 }) // Shorter TTL for filtered data

// Helper: Convert DD/MM/YYYY or DD/MM/YYYY HH:MM:SS to YYYY-MM-DD (optimized)
function convertToStandardDate(dateStr) {
  if (!dateStr) return null
  
  const str = dateStr.toString().trim()
  if (str === "") return null
  
  // Remove time part (everything after space)
  let datePart = str.split(' ')[0]
  
  // Check if format is DD/MM/YYYY
  if (datePart.includes('/')) {
    const parts = datePart.split('/')
    if (parts.length === 3) {
      // parts[0] = day, parts[1] = month, parts[2] = year
      const year = parts[2]
      const month = parts[1].padStart(2, '0')
      const day = parts[0].padStart(2, '0')
      return `${year}-${month}-${day}`
    }
  }
  
  // Check if already in YYYY-MM-DD format
  if (datePart.includes('-')) {
    return datePart
  }
  
  return null
}

// Helper: Get date only in YYYY-MM-DD format from any input
function getDateOnly(dateString) {
  if (!dateString || dateString.toString().trim() === "") return null
  
  const str = dateString.toString().trim()
  
  // Try direct conversion first
  let converted = convertToStandardDate(str)
  if (converted) return converted
  
  // Fallback: Try JavaScript Date object
  try {
    let date = new Date(str)
    if (!isNaN(date.getTime())) {
      return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`
    }
  } catch (e) {
    return null
  }
  
  return null
}

// Optimized: Get all sheet data with caching
async function getSheetData(forceRefresh = false) {
  const cacheKey = 'sheet_data'
  
  if (!forceRefresh) {
    const cachedData = dataCache.get(cacheKey)
    if (cachedData) {
      console.log("📦 Using cached sheet data")
      return cachedData
    }
  }
  
  console.log("🔄 Fetching fresh sheet data...")
  const sheets = await getSheets()
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEET_ID,
    range: `${process.env.SHEET_NAME}!A8:AU`,
    majorDimension: 'ROWS'
  })
  
  const rows = response.data.values || []
  
  // Pre-process and index the data for faster filtering
  const indexedData = {
    rows: rows,
    lastUpdated: Date.now(),
    // Create indexes for faster lookups
    partyIndex: new Map(),
    consigneeIndex: new Map(),
    billNumberIndex: new Map()
  }
  
  // Build indexes
  rows.forEach((row, index) => {
    const party = row[3]
    const consignee = row[4]
    const billNo = row[2]
    
    if (!indexedData.partyIndex.has(party)) {
      indexedData.partyIndex.set(party, [])
    }
    indexedData.partyIndex.get(party).push(index)
    
    if (!indexedData.consigneeIndex.has(consignee)) {
      indexedData.consigneeIndex.set(consignee, [])
    }
    indexedData.consigneeIndex.get(consignee).push(index)
    
    if (billNo) {
      indexedData.billNumberIndex.set(billNo, index)
    }
  })
  
  dataCache.set(cacheKey, indexedData, 60) // Cache for 60 seconds
  return indexedData
}

// ============================================================
// GET FILTERED DATA - OPTIMIZED VERSION
// ============================================================
router.get("/", async (req, res) => {
  const startTime = Date.now()
  
  try {
    console.log("\n" + "=".repeat(60))
    console.log("📊 GET FILTERED DATA (Optimized)")
    console.log("=".repeat(60))
    
    const { startDate, endDate, parties, consignees } = req.query
    
    // Create cache key based on query parameters
    const cacheKey = `filtered_${startDate || 'none'}_${endDate || 'none'}_${parties || 'none'}_${consignees || 'none'}`
    
    // Check cache first
    const cachedResult = cache.get(cacheKey)
    if (cachedResult) {
      console.log("✅ Returning cached result")
      console.log(`⏱️ Response time: ${Date.now() - startTime}ms (cached)`)
      return res.json(cachedResult)
    }
    
    const partyList = parties ? parties.split(",") : []
    const consigneeList = consignees ? consignees.split(",") : []
    
    console.log(`📅 Date Filter: ${startDate || 'NO'} to ${endDate || 'NO'}`)
    console.log(`👥 Parties: ${partyList.length > 0 ? partyList.length : 'ALL'}`)
    console.log(`👥 Consignees: ${consigneeList.length > 0 ? consigneeList.length : 'ALL'}`)
    
    // Get indexed sheet data
    const indexedData = await getSheetData()
    const rows = indexedData.rows
    
    console.log(`📄 Total rows in sheet: ${rows.length}`)
    
    // ========== DATE RANGE PREPARATION (INCLUSIVE START AND END) ==========
    let startTimestamp = null, endTimestamp = null
    if (startDate && endDate) {
      const startDateObj = new Date(startDate)
      startDateObj.setHours(0, 0, 0, 0)
      startTimestamp = startDateObj.getTime()
      
      const endDateObj = new Date(endDate)
      endDateObj.setHours(23, 59, 59, 999)
      endTimestamp = endDateObj.getTime()
    }
    
    // Optimized filtering with early exits
    let filtered = []
    
    // Determine which rows to process based on filters
    let rowsToProcess = null
    
    if (partyList.length > 0) {
      // Use party index for faster filtering
      rowsToProcess = new Set()
      partyList.forEach(party => {
        const indices = indexedData.partyIndex.get(party)
        if (indices) {
          indices.forEach(idx => rowsToProcess.add(idx))
        }
      })
    }
    
    if (consigneeList.length > 0 && rowsToProcess) {
      // Intersection with consignee index
      const finalSet = new Set()
      consigneeList.forEach(consignee => {
        const indices = indexedData.consigneeIndex.get(consignee)
        if (indices) {
          indices.forEach(idx => {
            if (rowsToProcess.has(idx)) {
              finalSet.add(idx)
            }
          })
        }
      })
      rowsToProcess = finalSet
    } else if (consigneeList.length > 0) {
      rowsToProcess = new Set()
      consigneeList.forEach(consignee => {
        const indices = indexedData.consigneeIndex.get(consignee)
        if (indices) {
          indices.forEach(idx => rowsToProcess.add(idx))
        }
      })
    }
    
    // Statistics counters
    let actual3EmptyCount = 0
    let plannedForLoopEmptyCount = 0
    
    // Process rows
    const rowsArray = rowsToProcess ? Array.from(rowsToProcess) : rows.map((_, idx) => idx)
    
    for (const idx of rowsArray) {
      const row = rows[idx]
      
      // Column mapping
      const plannedForLoopStr = row[21]   // Column V
      const party = row[3]                // Column D
      const consignee = row[4]            // Column E
      const actual3 = row[22]             // Column W
      
      // Condition 1: ACTUAL3 MUST BE EMPTY
      const isActual3Empty = !actual3 || actual3.toString().trim() === ""
      if (!isActual3Empty) continue
      actual3EmptyCount++
      
      // Condition 2: PLANNED FOR LOOP MUST NOT BE EMPTY
      const isPlannedForLoopEmpty = !plannedForLoopStr || plannedForLoopStr.toString().trim() === ""
      if (isPlannedForLoopEmpty) {
        plannedForLoopEmptyCount++
        continue
      }
      
      // Condition 3: Party filter (already handled by index, but double-check if no index)
      if (partyList.length > 0 && !partyList.includes(party)) continue
      
      // Condition 4: Consignee filter (already handled by index, but double-check if no index)
      if (consigneeList.length > 0 && !consigneeList.includes(consignee)) continue
      
      // Condition 5: Date range filter
      if (startTimestamp && endTimestamp) {
        let plannedDateObj = null
        
        try {
          let dateStr = plannedForLoopStr.toString().trim()
          let datePart = dateStr.split(' ')[0]
          
          if (datePart.includes('/')) {
            const parts = datePart.split('/')
            plannedDateObj = new Date(parts[2], parts[1] - 1, parts[0])
          } else if (datePart.includes('-')) {
            plannedDateObj = new Date(datePart)
          } else {
            plannedDateObj = new Date(dateStr)
          }
          
          if (isNaN(plannedDateObj.getTime())) continue
          
          plannedDateObj.setHours(0, 0, 0, 0)
          const plannedTimestamp = plannedDateObj.getTime()
          
          if (!(plannedTimestamp >= startTimestamp && plannedTimestamp <= endTimestamp)) continue
          
        } catch (e) {
          continue
        }
      }
      
      // Add to filtered results
      filtered.push({
        billNo: row[2] || "",
        party: party || "",
        consignee: consignee || "",
        billDate: row[1] || "",
        balance: row[6] || "0",
        plannedForLoop: plannedForLoopStr || "",
        followUp1: row[24] || "",
        followCount1: row[26] || "0",
        actual3: row[22] || ""
      })
    }
    
    console.log(`\n📈 FILTER STATISTICS:`)
    console.log(`   🎯 FINAL RESULT: ${filtered.length} records`)
    console.log(`⏱️ Processing time: ${Date.now() - startTime}ms`)
    
    // Cache the result for 15 seconds
    cache.set(cacheKey, filtered, 15)
    
    res.json(filtered)
    
  } catch (error) {
    console.error("❌ ERROR:", error)
    res.status(500).json({ error: "Filter failed: " + error.message })
  }
})

// ============================================================
// GET ALL PARTIES - OPTIMIZED WITH CACHE
// ============================================================
router.get("/parties", async (req, res) => {
  const startTime = Date.now()
  
  try {
    // Check cache
    const cachedParties = cache.get('all_parties')
    if (cachedParties) {
      console.log("✅ Returning cached parties")
      return res.json(cachedParties)
    }
    
    const indexedData = await getSheetData()
    const parties = [...indexedData.partyIndex.keys()].filter(Boolean).sort()
    
    console.log(`📋 Total unique parties: ${parties.length}`)
    console.log(`⏱️ Parties fetch time: ${Date.now() - startTime}ms`)
    
    // Cache for 5 minutes
    cache.set('all_parties', parties, 300)
    
    res.json(parties)
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to get parties" })
  }
})

// ============================================================
// GET CONSIGNEES BASED ON PARTIES - OPTIMIZED WITH CACHE
// ============================================================
router.get("/consignees", async (req, res) => {
  const startTime = Date.now()
  
  try {
    const { parties } = req.query
    const partyList = parties ? parties.split(",") : []
    
    // Create cache key based on parties
    const cacheKey = `consignees_${parties || 'all'}`
    const cachedConsignees = cache.get(cacheKey)
    
    if (cachedConsignees) {
      console.log("✅ Returning cached consignees")
      return res.json(cachedConsignees)
    }
    
    const indexedData = await getSheetData()
    let consigneesSet = new Set()
    
    if (partyList.length === 0) {
      // Get all consignees
      for (const [_, indices] of indexedData.consigneeIndex) {
        indices.forEach(idx => {
          const consignee = indexedData.rows[idx][4]
          if (consignee) consigneesSet.add(consignee)
        })
      }
    } else {
      // Get consignees for specific parties
      for (const party of partyList) {
        const partyIndices = indexedData.partyIndex.get(party)
        if (partyIndices) {
          partyIndices.forEach(idx => {
            const consignee = indexedData.rows[idx][4]
            if (consignee) consigneesSet.add(consignee)
          })
        }
      }
    }
    
    const uniqueConsignees = Array.from(consigneesSet).sort()
    console.log(`📋 Found ${uniqueConsignees.length} consignees for selected parties`)
    console.log(`⏱️ Consignees fetch time: ${Date.now() - startTime}ms`)
    
    // Cache for 2 minutes
    cache.set(cacheKey, uniqueConsignees, 120)
    
    res.json(uniqueConsignees)
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to get consignees" })
  }
})

// ============================================================
// SINGLE FOLLOW-UP UPDATE - OPTIMIZED
// ============================================================
router.post("/update-followup-single", async (req, res) => {
  try {
    const sheets = await getSheets()
    const { billNumber, followUpDate } = req.body
    
    console.log(`✏️ Single update: ${billNumber} -> ${followUpDate}`)
    
    // Try to get from cache first
    const indexedData = await getSheetData()
    const rowIndex = indexedData.billNumberIndex.get(billNumber)
    
    if (rowIndex !== undefined) {
      const sheetRow = rowIndex + 8
      const row = indexedData.rows[rowIndex]
      const followCount = parseInt(row[26] || "0")
      
      const formattedDate = getDateOnly(followUpDate) || followUpDate
      
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: process.env.GOOGLE_SHEET_ID,
        requestBody: {
          valueInputOption: "USER_ENTERED",
          data: [
            { range: `${process.env.SHEET_NAME}!Y${sheetRow}`, values: [[formattedDate]] },
            { range: `${process.env.SHEET_NAME}!AA${sheetRow}`, values: [[followCount + 1]] }
          ]
        }
      })
      
      // Invalidate caches after update
      cache.flushAll()
      dataCache.del('sheet_data')
      
      console.log(`✅ Updated successfully`)
      res.json({ success: true })
    } else {
      console.log(`❌ Bill number ${billNumber} not found`)
      res.json({ success: false })
    }
  } catch (err) {
    console.error("❌ Error:", err)
    res.status(500).json({ error: "Update failed: " + err.message })
  }
})

// ============================================================
// BULK FOLLOW-UP UPDATE - OPTIMIZED
// ============================================================
router.post("/update-followup", async (req, res) => {
  try {
    const sheets = await getSheets()
    const { billNumbers, followUpDate } = req.body
    
    console.log(`✏️ Bulk update: ${billNumbers.length} bills -> ${followUpDate}`)
    
    const indexedData = await getSheetData()
    const formattedDate = getDateOnly(followUpDate) || followUpDate
    
    let updates = []
    let updatedCount = 0
    
    for (const billNumber of billNumbers) {
      const rowIndex = indexedData.billNumberIndex.get(billNumber)
      if (rowIndex !== undefined) {
        const sheetRow = rowIndex + 8
        const row = indexedData.rows[rowIndex]
        const followCount = parseInt(row[26] || "0")
        
        updates.push(
          { range: `${process.env.SHEET_NAME}!Y${sheetRow}`, values: [[formattedDate]] },
          { range: `${process.env.SHEET_NAME}!AA${sheetRow}`, values: [[followCount + 1]] }
        )
        updatedCount++
      }
    }
    
    if (updates.length > 0) {
      // Batch updates in chunks of 50 to avoid API limits
      const chunkSize = 50
      for (let i = 0; i < updates.length; i += chunkSize) {
        const chunk = updates.slice(i, i + chunkSize)
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: process.env.GOOGLE_SHEET_ID,
          requestBody: {
            valueInputOption: "USER_ENTERED",
            data: chunk
          }
        })
      }
    }
    
    // Invalidate caches after update
    cache.flushAll()
    dataCache.del('sheet_data')
    
    console.log(`✅ Updated ${updatedCount} bills`)
    res.json({ success: true, updatedCount: updatedCount })
  } catch (err) {
    console.error("❌ Error:", err)
    res.status(500).json({ error: "Update failed: " + err.message })
  }
})

// ============================================================
// CLEAR CACHE ENDPOINT (for debugging)
// ============================================================
router.post("/clear-cache", (req, res) => {
  cache.flushAll()
  dataCache.del('sheet_data')
  console.log("🗑️ Cache cleared")
  res.json({ success: true, message: "Cache cleared" })
})

module.exports = router