const router = require("express").Router()
const getSheets = require("../googleSheet")
const NodeCache = require("node-cache")

// Initialize cache with different TTLs
const cache = new NodeCache({ stdTTL: 300, checkperiod: 120 })
const dataCache = new NodeCache({ stdTTL: 30, checkperiod: 60 })

// Helper: Normalize string (remove extra spaces, handle commas)
function normalizeString(str) {
  if (!str) return ""
  return str
    .toString()
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/ ,/g, ",")
    .replace(/, /g, ",")
    .replace(/\s*,\s*/g, ",")
    .trim()
}

// Helper: Parse special parameter (handles ||| separator and commas)
function parseSpecialParam(paramStr) {
  if (!paramStr || paramStr === 'none' || paramStr === '') return []
  
  // Decode first
  const decoded = decodeURIComponent(paramStr)
  
  // Check if using special separator '|||'
  if (decoded.includes('|||')) {
    return decoded.split('|||').map(p => p.trim())
  }
  
  // Single value
  return [decoded.trim()]
}

// Helper: Convert DD/MM/YYYY to YYYY-MM-DD
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
    normalizedPartyIndex: new Map(),
    consigneeIndex: new Map(),
    normalizedConsigneeIndex: new Map(),
    billNumberIndex: new Map()
  }
  
  // Build indexes
  rows.forEach((row, index) => {
    const party = row[3]
    const consignee = row[4]
    const billNo = row[2]
    
    if (party) {
      if (!indexedData.partyIndex.has(party)) {
        indexedData.partyIndex.set(party, [])
      }
      indexedData.partyIndex.get(party).push(index)
      
      const normalizedParty = normalizeString(party)
      if (!indexedData.normalizedPartyIndex.has(normalizedParty)) {
        indexedData.normalizedPartyIndex.set(normalizedParty, [])
      }
      indexedData.normalizedPartyIndex.get(normalizedParty).push(index)
    }
    
    if (consignee) {
      if (!indexedData.consigneeIndex.has(consignee)) {
        indexedData.consigneeIndex.set(consignee, [])
      }
      indexedData.consigneeIndex.get(consignee).push(index)
      
      const normalizedConsignee = normalizeString(consignee)
      if (!indexedData.normalizedConsigneeIndex.has(normalizedConsignee)) {
        indexedData.normalizedConsigneeIndex.set(normalizedConsignee, [])
      }
      indexedData.normalizedConsigneeIndex.get(normalizedConsignee).push(index)
    }
    
    if (billNo) {
      indexedData.billNumberIndex.set(billNo, index)
    }
  })
  
  dataCache.set(cacheKey, indexedData, 60)
  return indexedData
}

// ============================================================
// GET FILTERED DATA
// ============================================================
router.get("/", async (req, res) => {
  const startTime = Date.now()
  
  try {
    const { startDate, endDate, parties, consignees, skipCache } = req.query
    
    const cacheKey = `filtered_${startDate || 'none'}_${endDate || 'none'}_${parties || 'none'}_${consignees || 'none'}`
    
    if (skipCache !== 'true') {
      const cachedResult = cache.get(cacheKey)
      if (cachedResult) {
        console.log(`✅ Returning cached result (${cachedResult.length} records)`)
        return res.json(cachedResult)
      }
    }
    
    // Parse parameters
    const partyListRaw = parseSpecialParam(parties || '')
    const consigneeListRaw = parseSpecialParam(consignees || '')
    
    // Normalize for matching
    const partyListNorm = partyListRaw.map(p => normalizeString(p))
    const consigneeListNorm = consigneeListRaw.map(c => normalizeString(c))
    
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
    
    const filtered = []
    
    for (let idx = 0; idx < rows.length; idx++) {
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
      if (partyListRaw.length > 0) {
        if (!party) continue
        const normalizedParty = normalizeString(party)
        if (!partyListNorm.includes(normalizedParty)) continue
      }
      
      // Condition 4: Consignee filter
      if (consigneeListRaw.length > 0) {
        if (!consignee) continue
        const normalizedConsignee = normalizeString(consignee)
        if (!consigneeListNorm.includes(normalizedConsignee)) continue
      }
      
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
    
    cache.set(cacheKey, filtered, 15)
    res.json(filtered)
    
  } catch (error) {
    console.error("❌ ERROR:", error)
    res.status(500).json({ error: "Filter failed: " + error.message })
  }
})

// ============================================================
// GET ALL PARTIES
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
// GET CONSIGNEES - FIXED
// ============================================================
router.get("/consignees", async (req, res) => {
  const startTime = Date.now()
  
  try {
    const { parties } = req.query
    const partyListRaw = parseSpecialParam(parties || '')
    
    const cacheKey = `consignees_${parties || 'all'}`
    const cachedConsignees = cache.get(cacheKey)
    
    if (cachedConsignees) {
      console.log(`✅ Returning cached consignees (${cachedConsignees.length})`)
      return res.json(cachedConsignees)
    }
    
    const indexedData = await getSheetData()
    const consigneesMap = new Map() // normalized key -> original name
    
    console.log(`\n🔍 Fetching consignees for parties:`, partyListRaw)
    
    if (partyListRaw.length === 0) {
      // No filter - return ALL unique consignees
      console.log("🔍 Fetching ALL consignees...")
      for (const row of indexedData.rows) {
        const consignee = row[4]
        if (consignee && consignee.toString().trim() !== "") {
          const original = consignee.toString().trim()
          const key = normalizeString(original)
          if (!consigneesMap.has(key)) {
            consigneesMap.set(key, original)
          }
        }
      }
    } else {
      // With party filter
      const partyListNorm = partyListRaw.map(p => normalizeString(p))
      
      // Find all rows matching the parties
      for (let idx = 0; idx < indexedData.rows.length; idx++) {
        const row = indexedData.rows[idx]
        const dealer = row[3]
        const consignee = row[4]
        
        if (!dealer || !consignee) continue
        
        const normalizedDealer = normalizeString(dealer)
        
        if (partyListNorm.includes(normalizedDealer)) {
          const originalConsignee = consignee.toString().trim()
          const key = normalizeString(originalConsignee)
          if (!consigneesMap.has(key)) {
            consigneesMap.set(key, originalConsignee)
            console.log(`  Found: Dealer="${dealer}" -> Consignee="${originalConsignee}"`)
          }
        }
      }
    }
    
    const uniqueConsignees = Array.from(consigneesMap.values()).sort()
    console.log(`\n📋 Total unique consignees: ${uniqueConsignees.length} in ${Date.now() - startTime}ms`)
    
    cache.set(cacheKey, uniqueConsignees, 120)
    res.json(uniqueConsignees)
    
  } catch (error) {
    console.error("❌ Error in /consignees:", error)
    res.status(500).json({ error: "Failed to get consignees: " + error.message })
  }
})

// ============================================================
// SINGLE FOLLOW-UP UPDATE - FIXED (No batchUpdate)
// ============================================================
router.post("/update-followup-single", async (req, res) => {
  try {
    const sheets = await getSheets()
    const { billNumber, followUpDate } = req.body
    
    if (!billNumber) {
      return res.status(400).json({ error: "Bill number is required" })
    }
    
    console.log(`✏️ Single update: ${billNumber} -> ${followUpDate}`)
    
    const indexedData = await getSheetData(true)
    const rowIndex = indexedData.billNumberIndex.get(billNumber)
    
    if (rowIndex !== undefined) {
      const sheetRow = rowIndex + 8
      const row = indexedData.rows[rowIndex]
      const followCount = parseInt(row[26] || "0")
      
      const formattedDate = getDateOnly(followUpDate) || followUpDate || new Date().toISOString().split('T')[0]
      
      // FIX: Use separate update calls instead of batchUpdate
      await sheets.spreadsheets.values.update({
        spreadsheetId: process.env.GOOGLE_SHEET_ID,
        range: `${process.env.SHEET_NAME}!Y${sheetRow}`,
        valueInputOption: "USER_ENTERED",
        requestBody: {
          values: [[formattedDate]]
        }
      })
      
      await sheets.spreadsheets.values.update({
        spreadsheetId: process.env.GOOGLE_SHEET_ID,
        range: `${process.env.SHEET_NAME}!AA${sheetRow}`,
        valueInputOption: "USER_ENTERED",
        requestBody: {
          values: [[followCount + 1]]
        }
      })
      
      cache.flushAll()
      dataCache.del('sheet_data')
      
      console.log(`✅ Updated successfully`)
      res.json({ success: true, message: "Follow-up updated successfully" })
    } else {
      console.log(`❌ Bill number ${billNumber} not found`)
      res.status(404).json({ success: false, error: "Bill number not found" })
    }
  } catch (err) {
    console.error("❌ Error:", err)
    res.status(500).json({ error: "Update failed: " + err.message })
  }
})

// ============================================================
// BULK FOLLOW-UP UPDATE - FIXED (No batchUpdate, with delay)
// ============================================================
router.post("/update-followup", async (req, res) => {
  try {
    const sheets = await getSheets()
    const { billNumbers, followUpDate } = req.body
    
    if (!billNumbers || !Array.isArray(billNumbers) || billNumbers.length === 0) {
      return res.status(400).json({ error: "Valid bill numbers array is required" })
    }
    
    console.log(`✏️ Bulk update: ${billNumbers.length} bills -> ${followUpDate}`)
    
    const indexedData = await getSheetData(true)
    const formattedDate = getDateOnly(followUpDate) || followUpDate || new Date().toISOString().split('T')[0]
    
    let updatedCount = 0
    const notFound = []
    const errors = []
    
    // FIX: Use separate update calls with delay
    for (const billNumber of billNumbers) {
      const rowIndex = indexedData.billNumberIndex.get(billNumber)
      if (rowIndex !== undefined) {
        const sheetRow = rowIndex + 8
        const row = indexedData.rows[rowIndex]
        const followCount = parseInt(row[26] || "0")
        
        try {
          // Update Y column
          await sheets.spreadsheets.values.update({
            spreadsheetId: process.env.GOOGLE_SHEET_ID,
            range: `${process.env.SHEET_NAME}!Y${sheetRow}`,
            valueInputOption: "USER_ENTERED",
            requestBody: {
              values: [[formattedDate]]
            }
          })
          
          // Update AA column
          await sheets.spreadsheets.values.update({
            spreadsheetId: process.env.GOOGLE_SHEET_ID,
            range: `${process.env.SHEET_NAME}!AA${sheetRow}`,
            valueInputOption: "USER_ENTERED",
            requestBody: {
              values: [[followCount + 1]]
            }
          })
          
          updatedCount++
          console.log(`✅ Updated bill: ${billNumber} at row ${sheetRow}`)
          
          // Add small delay to avoid rate limiting and expansion errors
          await new Promise(resolve => setTimeout(resolve, 200))
          
        } catch (err) {
          console.error(`❌ Error updating bill ${billNumber}:`, err.message)
          errors.push({ billNumber, error: err.message })
        }
      } else {
        notFound.push(billNumber)
        console.log(`⚠️ Bill number not found: ${billNumber}`)
      }
    }
    
    cache.flushAll()
    dataCache.del('sheet_data')
    
    console.log(`✅ Updated ${updatedCount} bills, ${notFound.length} not found, ${errors.length} errors`)
    
    res.json({ 
      success: true, 
      updatedCount: updatedCount, 
      notFound: notFound,
      errors: errors,
      message: `Updated ${updatedCount} bills successfully` 
    })
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
  res.json({ success: true, message: "Cache cleared successfully" })
})

// ============================================================
// GET CACHE STATUS
// ============================================================
router.get("/cache-status", (req, res) => {
  const stats = {
    cacheKeys: cache.keys(),
    cacheSize: cache.keys().length,
    cacheStats: cache.getStats(),
    dataCacheKeys: dataCache.keys(),
    dataCacheSize: dataCache.keys().length,
    timestamp: new Date().toISOString()
  }
  res.json(stats)
})

// ============================================================
// HEALTH CHECK
// ============================================================
router.get("/health", (req, res) => {
  res.json({ 
    status: "OK", 
    timestamp: new Date().toISOString(),
    uptime: process.uptime()
  })
})

module.exports = router