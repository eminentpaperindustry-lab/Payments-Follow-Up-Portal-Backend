const router = require("express").Router()
const getSheets = require("../googleSheet")
const NodeCache = require("node-cache")

// Initialize cache with different TTLs
const cache = new NodeCache({ stdTTL: 300, checkperiod: 120 })
const dataCache = new NodeCache({ stdTTL: 30, checkperiod: 60 })

// Helper: Convert DD/MM/YYYY to YYYY-MM-DD (optimized)
function convertToStandardDate(dateStr) {
  if (!dateStr) return null
  
  const str = dateStr.toString().trim()
  if (str === "") return null
  
  const datePart = str.split(' ')[0]
  
  if (datePart.includes('/')) {
    const parts = datePart.split('/')
    if (parts.length === 3) {
      const year = parts[2]
      const month = parts[1].padStart(2, '0')
      const day = parts[0].padStart(2, '0')
      return `${year}-${month}-${day}`
    }
  }
  
  if (datePart.includes('-')) {
    return datePart
  }
  
  return null
}

function getDateOnly(dateString) {
  if (!dateString || dateString.toString().trim() === "") return null
  
  const str = dateString.toString().trim()
  
  let converted = convertToStandardDate(str)
  if (converted) return converted
  
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
  
  const indexedData = {
    rows: rows,
    lastUpdated: Date.now(),
    partyIndex: new Map(),
    consigneeIndex: new Map(),
    billNumberIndex: new Map()
  }
  
  // Build indexes
  rows.forEach((row, index) => {
    const party = row[3]
    const consignee = row[4]
    const billNo = row[2]
    
    if (party && !indexedData.partyIndex.has(party)) {
      indexedData.partyIndex.set(party, [])
    }
    if (party) {
      indexedData.partyIndex.get(party).push(index)
    }
    
    if (consignee && !indexedData.consigneeIndex.has(consignee)) {
      indexedData.consigneeIndex.set(consignee, [])
    }
    if (consignee) {
      indexedData.consigneeIndex.get(consignee).push(index)
    }
    
    if (billNo) {
      indexedData.billNumberIndex.set(billNo, index)
    }
  })
  
  dataCache.set(cacheKey, indexedData, 60)
  return indexedData
}

// ============================================================
// GET FILTERED DATA - FAST OPTIMIZED VERSION
// ============================================================
router.get("/", async (req, res) => {
  const startTime = Date.now()
  
  try {
    const { startDate, endDate, parties, consignees, skipCache } = req.query
    
    const cacheKey = `filtered_${startDate || 'none'}_${endDate || 'none'}_${parties || 'none'}_${consignees || 'none'}`
    
    // Skip cache if requested
    if (skipCache !== 'true') {
      const cachedResult = cache.get(cacheKey)
      if (cachedResult) {
        console.log(`✅ Returning cached result (${cachedResult.length} records)`)
        console.log(`⏱️ Response time: ${Date.now() - startTime}ms (cached)`)
        return res.json(cachedResult)
      }
    }
    
    const partyList = parties && parties !== 'none' ? parties.split(",") : []
    const consigneeList = consignees && consignees !== 'none' ? consignees.split(",") : []
    
    const indexedData = await getSheetData()
    const rows = indexedData.rows
    
    // Date range preparation
    let startTimestamp = null, endTimestamp = null
    if (startDate && endDate && startDate !== 'none' && endDate !== 'none') {
      const startDateObj = new Date(startDate)
      startDateObj.setHours(0, 0, 0, 0)
      startTimestamp = startDateObj.getTime()
      
      const endDateObj = new Date(endDate)
      endDateObj.setHours(23, 59, 59, 999)
      endTimestamp = endDateObj.getTime()
    }
    
    // Determine rows to process
    let rowsToProcess = null
    
    if (partyList.length > 0) {
      rowsToProcess = new Set()
      partyList.forEach(party => {
        const indices = indexedData.partyIndex.get(party)
        if (indices) {
          indices.forEach(idx => rowsToProcess.add(idx))
        }
      })
    }
    
    if (consigneeList.length > 0 && rowsToProcess) {
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
    
    // Process rows
    const rowsArray = rowsToProcess ? Array.from(rowsToProcess) : rows.map((_, idx) => idx)
    const filtered = []
    
    for (const idx of rowsArray) {
      const row = rows[idx]
      
      const plannedForLoopStr = row[21]
      const party = row[3]
      const consignee = row[4]
      const actual3 = row[22]
      
      // Condition 1: ACTUAL3 must be empty
      if (actual3 && actual3.toString().trim() !== "") continue
      
      // Condition 2: PLANNED FOR LOOP must not be empty
      if (!plannedForLoopStr || plannedForLoopStr.toString().trim() === "") continue
      
      // Condition 3: Party filter
      if (partyList.length > 0 && !partyList.includes(party)) continue
      
      // Condition 4: Consignee filter
      if (consigneeList.length > 0 && !consigneeList.includes(consignee)) continue
      
      // Condition 5: Date range filter
      if (startTimestamp && endTimestamp) {
        try {
          let dateStr = plannedForLoopStr.toString().trim()
          let datePart = dateStr.split(' ')[0]
          let plannedDateObj = null
          
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
      
      filtered.push({
        billNo: row[2] || "",
        party: party || "",
        consignee: consignee || "",
        billDate: row[1] || "",
        balance: row[6] || "0",
        plannedForLoop: plannedForLoopStr || "",
        followUp1: row[24] || "",
        BalanceRemaining: row[12] || "0",
        followCount1: row[26] || "0",
        actual3: row[22] || ""
      })
    }
    
    console.log(`🎯 FINAL RESULT: ${filtered.length} records in ${Date.now() - startTime}ms`)
    
    // Cache for 15 seconds
    cache.set(cacheKey, filtered, 15)
    
    res.json(filtered)
    
  } catch (error) {
    console.error("❌ ERROR:", error)
    res.status(500).json({ error: "Filter failed: " + error.message })
  }
})

// ============================================================
// GET ALL PARTIES - FAST WITH LONG CACHE
// ============================================================
router.get("/parties", async (req, res) => {
  const startTime = Date.now()
  
  try {
    const cachedParties = cache.get('all_parties')
    if (cachedParties) {
      console.log(`✅ Returning cached parties (${cachedParties.length})`)
      return res.json(cachedParties)
    }
    
    const indexedData = await getSheetData()
    const parties = [...indexedData.partyIndex.keys()].filter(Boolean).sort()
    
    console.log(`📋 Total unique parties: ${parties.length} in ${Date.now() - startTime}ms`)
    
    cache.set('all_parties', parties, 300)
    
    res.json(parties)
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to get parties" })
  }
})

// ============================================================
// GET CONSIGNEES - FAST WITH CACHE
// ============================================================
router.get("/consignees", async (req, res) => {
  const startTime = Date.now()
  
  try {
    const { parties } = req.query
    const partyList = parties ? parties.split(",") : []
    
    const cacheKey = `consignees_${parties || 'all'}`
    const cachedConsignees = cache.get(cacheKey)
    
    if (cachedConsignees) {
      console.log(`✅ Returning cached consignees (${cachedConsignees.length})`)
      return res.json(cachedConsignees)
    }
    
    const indexedData = await getSheetData()
    const consigneesSet = new Set()
    
    if (partyList.length === 0) {
      for (const [consignee, indices] of indexedData.consigneeIndex) {
        if (consignee) consigneesSet.add(consignee)
      }
    } else {
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
    console.log(`📋 Found ${uniqueConsignees.length} consignees in ${Date.now() - startTime}ms`)
    
    cache.set(cacheKey, uniqueConsignees, 120)
    
    res.json(uniqueConsignees)
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to get consignees" })
  }
})

// ============================================================
// SINGLE FOLLOW-UP UPDATE
// ============================================================
router.post("/update-followup-single", async (req, res) => {
  try {
    const sheets = await getSheets()
    const { billNumber, followUpDate } = req.body
    
    console.log(`✏️ Single update: ${billNumber} -> ${followUpDate}`)
    
    const indexedData = await getSheetData(true) // Force refresh
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
      
      // Clear all caches
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
// BULK FOLLOW-UP UPDATE - FAST BATCH PROCESSING
// ============================================================
router.post("/update-followup", async (req, res) => {
  try {
    const sheets = await getSheets()
    const { billNumbers, followUpDate } = req.body
    
    console.log(`✏️ Bulk update: ${billNumbers.length} bills -> ${followUpDate}`)
    
    const indexedData = await getSheetData(true) // Force refresh
    const formattedDate = getDateOnly(followUpDate) || followUpDate
    
    const updates = []
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
    
    // Clear all caches
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
// CLEAR CACHE ENDPOINT
// ============================================================
router.post("/clear-cache", (req, res) => {
  cache.flushAll()
  dataCache.del('sheet_data')
  console.log("🗑️ All cache cleared")
  res.json({ success: true, message: "Cache cleared" })
})

// ============================================================
// GET CACHE STATUS (for debugging)
// ============================================================
router.get("/cache-status", (req, res) => {
  const stats = {
    cacheKeys: cache.keys(),
    cacheSize: cache.keys().length,
    dataCacheKeys: dataCache.keys(),
    dataCacheSize: dataCache.keys().length
  }
  res.json(stats)
})

module.exports = router