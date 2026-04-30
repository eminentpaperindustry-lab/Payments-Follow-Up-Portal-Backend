const router = require("express").Router()
const getSheets = require("../googleSheet")

// Helper: Convert DD/MM/YYYY or DD/MM/YYYY HH:MM:SS to YYYY-MM-DD
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

// ============================================================
// GET FILTERED DATA - BETWEEN START DATE AND END DATE
// ============================================================
router.get("/", async (req, res) => {
  try {
    console.log("\n" + "=".repeat(60))
    console.log("📊 GET FILTERED DATA")
    console.log("=".repeat(60))
    
    const sheets = await getSheets()
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: `${process.env.SHEET_NAME}!A8:AU`
    })

    let rows = response.data.values || []
    const { startDate, endDate, parties, consignees } = req.query

    const partyList = parties ? parties.split(",") : []
    const consigneeList = consignees ? consignees.split(",") : []

    console.log(`📅 Date Filter: ${startDate || 'NO'} to ${endDate || 'NO'}`)
    console.log(`👥 Parties: ${partyList.length > 0 ? partyList.length : 'ALL'}`)
    console.log(`👥 Consignees: ${consigneeList.length > 0 ? consigneeList.length : 'ALL'}`)
    console.log(`📄 Total rows in sheet: ${rows.length}`)

    // ========== DATE RANGE PREPARATION (INCLUSIVE START AND END) ==========
    let startTimestamp = null, endTimestamp = null
    if (startDate && endDate) {
      // Convert start date to beginning of day (00:00:00)
      const startDateObj = new Date(startDate)
      startDateObj.setHours(0, 0, 0, 0)
      startTimestamp = startDateObj.getTime()
      
      // Convert end date to end of day (23:59:59)
      const endDateObj = new Date(endDate)
      endDateObj.setHours(23, 59, 59, 999)
      endTimestamp = endDateObj.getTime()
      
      console.log(`📅 Date Range (Timestamp): ${new Date(startTimestamp)} to ${new Date(endTimestamp)}`)
    }

    // Statistics counters
    let totalRows = 0
    let actual3EmptyCount = 0
    let plannedForLoopEmptyCount = 0
    let plannedForLoopInvalidCount = 0
    let partyMatchCount = 0
    let consigneeMatchCount = 0
    let dateMatchCount = 0

    const filtered = rows.filter(row => {
      totalRows++
      
      // ========== COLUMN MAPPING ==========
      const plannedForLoopStr = row[21]   // Column V - Planned For Loop (DATE)
      const party = row[3]                // Column D - Dealer Name
      const consignee = row[4]            // Column E - Consignee name
      const actual3 = row[22]             // Column W - Actual3

      // ========== CONDITION 1: ACTUAL3 MUST BE EMPTY ==========
      const isActual3Empty = !actual3 || actual3.toString().trim() === ""
      if (isActual3Empty) actual3EmptyCount++
      if (!isActual3Empty) return false

      // ========== CONDITION 2: PLANNED FOR LOOP MUST NOT BE EMPTY ==========
      const isPlannedForLoopEmpty = !plannedForLoopStr || plannedForLoopStr.toString().trim() === ""
      if (isPlannedForLoopEmpty) {
        plannedForLoopEmptyCount++
        return false
      }

      // ========== CONDITION 3: PARTY FILTER ==========
      if (partyList.length > 0 && !partyList.includes(party)) return false
      partyMatchCount++

      // ========== CONDITION 4: CONSIGNEE FILTER ==========
      if (consigneeList.length > 0 && !consigneeList.includes(consignee)) return false
      consigneeMatchCount++

      // ========== CONDITION 5: DATE RANGE FILTER (INCLUSIVE - FULL DAY) ==========
      if (startTimestamp && endTimestamp) {
        // Convert Planned For Loop string to proper Date object
        let plannedDateObj = null
        
        try {
          // Try parsing DD/MM/YYYY or DD/MM/YYYY HH:MM:SS format
          let dateStr = plannedForLoopStr.toString().trim()
          let datePart = dateStr.split(' ')[0] // Remove time if present
          
          if (datePart.includes('/')) {
            const parts = datePart.split('/')
            // parts[0] = day, parts[1] = month, parts[2] = year
            plannedDateObj = new Date(parts[2], parts[1] - 1, parts[0])
          } else if (datePart.includes('-')) {
            plannedDateObj = new Date(datePart)
          } else {
            plannedDateObj = new Date(dateStr)
          }
          
          // Check if date is valid
          if (isNaN(plannedDateObj.getTime())) {
            plannedForLoopInvalidCount++
            return false
          }
          
          // Set time to beginning of day for consistent comparison
          plannedDateObj.setHours(0, 0, 0, 0)
          const plannedTimestamp = plannedDateObj.getTime()
          
          // INCLUSIVE CHECK: planned date >= start date AND planned date <= end date
          const isInRange = plannedTimestamp >= startTimestamp && plannedTimestamp <= endTimestamp
          
          if (isInRange) dateMatchCount++
          return isInRange
          
        } catch (e) {
          plannedForLoopInvalidCount++
          return false
        }
      }

      dateMatchCount++
      return true
    })

    // ========== PRINT STATISTICS ==========
    console.log(`\n📈 FILTER STATISTICS:`)
    console.log(`   Total Rows Processed: ${totalRows}`)
    console.log(`   ✅ Actual3 Empty: ${actual3EmptyCount}`)
    console.log(`   ❌ Planned For Loop Empty: ${plannedForLoopEmptyCount}`)
    console.log(`   ❌ Planned For Loop Invalid Date: ${plannedForLoopInvalidCount}`)
    console.log(`   ✅ Party Match: ${partyMatchCount}`)
    console.log(`   ✅ Consignee Match: ${consigneeMatchCount}`)
    console.log(`   ✅ Date Match (in range): ${dateMatchCount}`)
    console.log(`   🎯 FINAL RESULT: ${filtered.length} records`)

    // ========== DEBUG: Show date parsing issues ==========
    if (plannedForLoopInvalidCount > 0) {
      console.log(`\n⚠️ DEBUG: Showing 3 rows with invalid date format:`)
      let shown = 0
      for (let i = 0; i < rows.length && shown < 3; i++) {
        const plannedForLoop = rows[i][21]
        const actual3 = rows[i][22]
        const isActual3Empty = !actual3 || actual3.toString().trim() === ""
        if (isActual3Empty && plannedForLoop) {
          let datePart = plannedForLoop.toString().trim().split(' ')[0]
          let testDate = new Date(datePart)
          if (isNaN(testDate.getTime()) && datePart.includes('/')) {
            console.log(`   Row ${i+8}: PlannedForLoop="${plannedForLoop}" - Format: DD/MM/YYYY`)
            shown++
          }
        }
      }
    }

    // ========== FORMAT OUTPUT ==========
    const formattedData = filtered.map(row => ({
      billNo: row[2] || "",
      party: row[3] || "",
      consignee: row[4] || "",
      billDate: row[1] || "",
      balance: row[6] || "0",
      plannedForLoop: row[21] || "",
      followUp1: row[24] || "",
      followCount1: row[26] || "0",
      actual3: row[22] || ""
    }))

    res.json(formattedData)
    
  } catch (error) {
    console.error("❌ ERROR:", error)
    res.status(500).json({ error: "Filter failed: " + error.message })
  }
})

// ============================================================
// GET ALL PARTIES
// ============================================================
router.get("/parties", async (req, res) => {
  try {
    const sheets = await getSheets()
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: `${process.env.SHEET_NAME}!A8:AU`
    })

    const rows = response.data.values || []
    const parties = rows.map(row => row[3]).filter(Boolean)
    const uniqueParties = [...new Set(parties)].sort()

    console.log(`📋 Total unique parties: ${uniqueParties.length}`)
    res.json(uniqueParties)
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: "Failed to get parties" })
  }
})

// ============================================================
// GET CONSIGNEES BASED ON PARTIES
// ============================================================
router.get("/consignees", async (req, res) => {
  try {
    const sheets = await getSheets()
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: `${process.env.SHEET_NAME}!A8:AU`
    })

    const rows = response.data.values || []
    const { parties } = req.query
    let partyList = parties ? parties.split(",") : []

    const consignees = rows
      .filter(row => partyList.length === 0 || partyList.includes(row[3]))
      .map(row => row[4])
      .filter(Boolean)

    const uniqueConsignees = [...new Set(consignees)].sort()
    console.log(`📋 Found ${uniqueConsignees.length} consignees for selected parties`)
    
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

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: `${process.env.SHEET_NAME}!A8:AU`
    })

    const rows = response.data.values || []
    let updated = false

    for (let i = 0; i < rows.length; i++) {
      if (rows[i][2] === billNumber) {
        const sheetRow = i + 8
        const followCount = parseInt(rows[i][26] || "0")
        
        // Convert followUpDate to proper format if needed
        const formattedDate = getDateOnly(followUpDate) || followUpDate

        console.log(`   Found at row ${sheetRow}, old count: ${followCount}`)

        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: process.env.GOOGLE_SHEET_ID,
          requestBody: {
            valueInputOption: "USER_ENTERED",
            data: [
              // { range: `${process.env.SHEET_NAME}!V${sheetRow}`, values: [[formattedDate]] },
              { range: `${process.env.SHEET_NAME}!Y${sheetRow}`, values: [[formattedDate]] },
              { range: `${process.env.SHEET_NAME}!AA${sheetRow}`, values: [[followCount + 1]] }
            ]
          }
        })
        updated = true
        console.log(`✅ Updated successfully`)
        break
      }
    }

    res.json({ success: updated })
  } catch (err) {
    console.error("❌ Error:", err)
    res.status(500).json({ error: "Update failed: " + err.message })
  }
})

// ============================================================
// BULK FOLLOW-UP UPDATE
// ============================================================
router.post("/update-followup", async (req, res) => {
  try {
    const sheets = await getSheets()
    const { billNumbers, followUpDate } = req.body

    console.log(`✏️ Bulk update: ${billNumbers.length} bills -> ${followUpDate}`)

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: `${process.env.SHEET_NAME}!A8:AU`
    })

    const rows = response.data.values || []
    let updates = []
    
    // Convert followUpDate to proper format if needed
    const formattedDate = getDateOnly(followUpDate) || followUpDate

    rows.forEach((row, index) => {
      if (billNumbers.includes(row[2])) {
        const sheetRow = index + 8
        const followCount = parseInt(row[26] || "0")

        updates.push(
          // { range: `${process.env.SHEET_NAME}!V${sheetRow}`, values: [[formattedDate]] },
          { range: `${process.env.SHEET_NAME}!Y${sheetRow}`, values: [[formattedDate]] },
          { range: `${process.env.SHEET_NAME}!AA${sheetRow}`, values: [[followCount + 1]] }
        )
      }
    })

    if (updates.length > 0) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: process.env.GOOGLE_SHEET_ID,
        requestBody: {
          valueInputOption: "USER_ENTERED",
          data: updates
        }
      })
    }

    console.log(`✅ Updated ${updates.length / 3} bills`)
    res.json({ success: true, updatedCount: updates.length / 3 })
  } catch (err) {
    console.error("❌ Error:", err)
    res.status(500).json({ error: "Update failed: " + err.message })
  }
})

module.exports = router