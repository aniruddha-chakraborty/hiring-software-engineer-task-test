package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// --- API Models ---
type LineItemCreate struct {
	Name         string   `json:"name,omitempty" validate:"required,min=1,max=100"`
	AdvertiserID string   `json:"advertiser_id,omitempty" validate:"required"`
	Bid          float64  `json:"bid,omitempty" validate:"required,gte=0.1,lte=10"`
	Budget       float64  `json:"budget,omitempty" validate:"required,gte=1000,lte=10000"`
	Placement    string   `json:"placement,omitempty" validate:"required,oneof=homepage_top video_preroll article_inline_1"`
	Categories   []string `json:"categories,omitempty"`
	Keywords     []string `json:"keywords,omitempty"`
}

type LineItem struct {
	LineItemCreate
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Status    string    `json:"status"`
}

type Ad struct {
	ID           string  `json:"id"`
	Name         string  `json:"name"`
	AdvertiserID string  `json:"advertiser_id"`
	Bid          float64 `json:"bid"`
	Placement    string  `json:"placement"`
	ServeURL     string  `json:"serve_url"`
	Relevance    float64 `json:"relevance"`
}

type TrackingEvent struct {
	EventType  string            `json:"event_type"`
	LineItemID string            `json:"line_item_id"`
	Timestamp  string            `json:"timestamp,omitempty"`
	Placement  string            `json:"placement,omitempty"`
	UserID     string            `json:"user_id,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// --- Main Application Logic ---

func main() {
	rand.Seed(time.Now().UnixNano())

	if len(os.Args) < 2 {
		printUsage()
		return
	}

	command := os.Args[1]
	switch command {
	case "create":
		runCreateLineItem()
	case "get-ad":
		runGetWinningAd()
	case "ad-test":
		runTargetedAdTests()
	case "validation-test":
		runValidationTests()
	case "e2e-tracking-test":
		runE2ETrackingTest()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
	}
}

// --- Helper and Data Functions ---

func getKnownLineItems() []LineItemCreate {
	return []LineItemCreate{
		{"Summer Sale Banner", "adv123", 2.5, 3000.0, "homepage_top", []string{"electronics", "sports"}, []string{"summer", "discount"}},
		{"Winter Clearance Promo", "adv456", 3.0, 7000.0, "video_preroll", []string{"fashion", "beauty"}, []string{"clearance", "deal"}},
		{"Travel Deals Campaign", "adv789", 1.8, 5000.0, "article_inline_1", []string{"travel", "food"}, []string{"exclusive", "trending"}},
		{"Gaming Weekend Blast", "adv321", 0.2, 8000.0, "homepage_top", []string{"gaming", "electronics"}, []string{"sale", "new"}},
		{"Home Essentials Discount", "adv654", 2.2, 10000.0, "video_preroll", []string{"home", "sports"}, []string{"deal", "discount"}},
		{"Back to School Deals", "adv111", 3.5, 4000.0, "article_inline_1", []string{"electronics", "fashion"}, []string{"exclusive", "sale"}},
		{"Spring Fashion Promo", "adv222", 1.9, 9000.0, "homepage_top", []string{"fashion", "beauty"}, []string{"trending", "new"}},
		{"Holiday Travel Specials", "adv333", 2.7, 6000.0, "video_preroll", []string{"travel", "food"}, []string{"deal", "exclusive"}},
		{"Fitness Gear Discount", "adv444", 2.3, 3000.0, "article_inline_1", []string{"sports", "home"}, []string{"discount", "sale"}},
		{"Luxury Beauty Sale", "adv555", 4.5, 7000.0, "homepage_top", []string{"beauty", "fashion"}, []string{"clearance", "exclusive"}},
		{"Gadget Madness", "adv666", 3.1, 5000.0, "video_preroll", []string{"electronics", "gaming"}, []string{"trending", "new"}},
		{"Healthy Living Promo", "adv777", 2.6, 8000.0, "article_inline_1", []string{"food", "home"}, []string{"deal", "sale"}},
		{"Weekend Getaway Deals", "adv888", 1.7, 9000.0, "homepage_top", []string{"travel", "sports"}, []string{"exclusive", "discount"}},
		{"Clearance Electronics", "adv999", 3.8, 1000.0, "video_preroll", []string{"electronics", "home"}, []string{"clearance", "deal"}},
		{"Gaming Console Offer", "adv112", 4.0, 6000.0, "article_inline_1", []string{"gaming", "electronics"}, []string{"new", "sale"}},
		{"Cozy Home Sale", "adv113", 2.4, 2000.0, "homepage_top", []string{"home", "fashion"}, []string{"discount", "exclusive"}},
		{"Fashion Week Promo", "adv114", 2.9, 3000.0, "video_preroll", []string{"fashion", "beauty"}, []string{"trending", "sale"}},
		{"Sports Gear Blowout", "adv115", 3.3, 5000.0, "article_inline_1", []string{"sports", "gaming"}, []string{"deal", "clearance"}},
		{"Smart Home Specials", "adv116", 2.1, 7000.0, "homepage_top", []string{"electronics", "home"}, []string{"exclusive", "new"}},
		{"Travel Light Promo", "adv117", 1.5, 4000.0, "video_preroll", []string{"travel", "fashion"}, []string{"discount", "sale"}},
	}
}

func createSingleLineItem(name, advertiserID string, bid, budget float64, placement string, categories, keywords []string) {
	item := LineItemCreate{
		Name:         name,
		AdvertiserID: advertiserID,
		Bid:          bid,
		Budget:       budget,
		Placement:    placement,
		Categories:   categories,
		Keywords:     keywords,
	}
	jsonData, _ := json.Marshal(item)
	resp, err := http.Post("http://localhost:8080/api/v1/lineitems", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Printf("FAIL: Could not create '%s'. Error: %v\n", name, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusCreated {
		fmt.Printf("SUCCESS: Created line item '%s'\n", name)
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("FAIL: Could not create '%s'. Status: %d, Body: %s\n", name, resp.StatusCode, string(body))
	}
}

func runCreateLineItem() {
	if len(os.Args) != 7 {
		fmt.Println("Error: 'create' command requires 5 arguments.")
		printUsage()
		return
	}
	bid, err := strconv.ParseFloat(os.Args[4], 64)
	if err != nil {
		fmt.Println("Error: Invalid bid amount. Must be a number.")
		return
	}
	budget, err := strconv.ParseFloat(os.Args[5], 64)
	if err != nil {
		fmt.Println("Error: Invalid budget amount. Must be a number.")
		return
	}
	newItem := LineItemCreate{
		Name:         os.Args[2],
		AdvertiserID: os.Args[3],
		Bid:          bid,
		Budget:       budget,
		Placement:    os.Args[6],
	}
	jsonData, _ := json.Marshal(newItem)
	resp, err := http.Post("http://localhost:8080/api/v1/lineitems", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer resp.Body.Close()
	fmt.Println("Server Response:")
	io.Copy(os.Stdout, resp.Body)
	fmt.Println()
}

func runGetWinningAd() {
	if len(os.Args) != 5 {
		fmt.Println("Error: 'get-ad' command requires 3 arguments: <placement> <category> <keyword>")
		printUsage()
		return
	}
	placement := os.Args[2]
	category := os.Args[3]
	keyword := os.Args[4]
	fmt.Printf("Fetching winning ad for placement='%s', category='%s', keyword='%s'...\n", placement, category, keyword)
	baseURL := "http://localhost:8080/api/v1/ads"
	params := url.Values{}
	params.Add("placement", placement)
	params.Add("category", category)
	params.Add("keyword", keyword)
	reqURL := fmt.Sprintf("%s?%s", baseURL, params.Encode())
	resp, err := http.Get(reqURL)
	if err != nil {
		fmt.Println("Error making request:", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Error: Server returned status %d. Body: %s\n", resp.StatusCode, string(body))
		return
	}
	var ads []Ad
	if err := json.NewDecoder(resp.Body).Decode(&ads); err != nil {
		fmt.Println("Error decoding response:", err)
		return
	}
	if len(ads) == 0 {
		fmt.Println("\nNo winning ads found for the given criteria.")
	} else {
		fmt.Println("\n--- Winning Ad(s) ---")
		for _, ad := range ads {
			fmt.Printf("  ID: %s, Name: %s, Bid: %.2f\n", ad.ID, ad.Name, ad.Bid)
		}
	}
}
func runTargetedAdTests() {
	fmt.Println("--- Running Simplified & Targeted Ad Logic Tests (First 5 Items) ---")
	allLineItems := getKnownLineItems()
	totalTests := 5
	passedCount := 0
	for i := 0; i < totalTests; i++ {
		itemToTest := allLineItems[i]
		fmt.Printf("\n--- [Test %d/%d] Testing for: '%s' ---\n", i+1, totalTests, itemToTest.Name)
		if len(itemToTest.Categories) == 0 || len(itemToTest.Keywords) == 0 {
			fmt.Println("  [SKIP] Item has no categories or keywords to test with.")
			continue
		}
		testPlacement := itemToTest.Placement
		testCategory := itemToTest.Categories[0]
		testKeyword := itemToTest.Keywords[0]
		fmt.Printf("  - Using its own targeting: placement='%s', category='%s', keyword='%s'\n", testPlacement, testCategory, testKeyword)
		predictedWinnerName := itemToTest.Name
		fmt.Printf("  - Prediction: The API should return '%s' in the list of ads.\n", predictedWinnerName)
		params := url.Values{}
		params.Add("placement", testPlacement)
		params.Add("category", testCategory)
		params.Add("keyword", testKeyword)
		params.Add("limit", "4")
		reqURL := fmt.Sprintf("http://localhost:8080/api/v1/ads?%s", params.Encode())
		fmt.Printf("  - CURL Command: curl -X GET '%s'\n", reqURL)
		fmt.Println("  - ACTION: Calling the real API...")
		resp, err := http.Get(reqURL)
		if err != nil {
			fmt.Printf("  [RESULT] ❌ FAIL: API call failed: %v\n", err)
			continue
		}
		defer resp.Body.Close()
		var actualAds []Ad
		if err := json.NewDecoder(resp.Body).Decode(&actualAds); err != nil {
			fmt.Printf("  [RESULT] ❌ FAIL: Could not decode API response: %v\n", err)
			continue
		}
		found := false
		for _, ad := range actualAds {
			if ad.Name == predictedWinnerName {
				found = true
				break
			}
		}
		if found {
			fmt.Printf("  [RESULT] ✅ PASS: Predicted ad '%s' was found in the returned list of %d ads.\n", predictedWinnerName, len(actualAds))
			passedCount++
		} else {
			if len(actualAds) == 0 {
				fmt.Printf("  [RESULT] ❌ FAIL: Predicted '%s', but API returned no ads.\n", predictedWinnerName)
			} else {
				fmt.Printf("  [RESULT] ❌ FAIL: Predicted ad '%s' was NOT found in the returned list.\n", predictedWinnerName)
			}
		}
	}
	fmt.Printf("\n--- Test Summary: %d/%d tests passed. ---\n", passedCount, totalTests)
}

func runValidationTests() {
	fmt.Println("--- Running Line Item Creation Validation Tests ---")
	passedCount := 0
	totalTests := 6
	runTestCase := func(testName string, payload LineItemCreate, expectedStatus int) {
		fmt.Printf("\n[TEST] %s\n", testName)
		jsonData, _ := json.Marshal(payload)
		resp, err := http.Post("http://localhost:8080/api/v1/lineitems", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Printf("  [RESULT] ❌ FAIL: API call failed: %v\n", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode == expectedStatus {
			fmt.Printf("  [RESULT] ✅ PASS: Received expected status %d\n", expectedStatus)
			passedCount++
		} else {
			body, _ := io.ReadAll(resp.Body)
			fmt.Printf("  [RESULT] ❌ FAIL: Expected status %d, but got %d. Body: %s\n", expectedStatus, resp.StatusCode, string(body))
		}
	}
	validItem := LineItemCreate{
		Name:         "Valid Test Item",
		AdvertiserID: "adv-valid",
		Bid:          5.0,
		Budget:       5000.0,
		Placement:    "homepage_top",
	}
	runTestCase("Creating a completely valid line item", validItem, http.StatusCreated)
	longNameItem := validItem
	longNameItem.Name = strings.Repeat("a", 101)
	runTestCase("Name longer than 100 characters", longNameItem, http.StatusBadRequest)
	lowBidItem := validItem
	lowBidItem.Bid = 0.05
	runTestCase("Bid less than 0.1", lowBidItem, http.StatusBadRequest)
	highBudgetItem := validItem
	highBudgetItem.Budget = 10001
	runTestCase("Budget greater than 10000", highBudgetItem, http.StatusBadRequest)
	invalidPlacementItem := validItem
	invalidPlacementItem.Placement = "this_is_not_a_valid_placement"
	runTestCase("Placement not in the allowed list", invalidPlacementItem, http.StatusBadRequest)
	missingNameItem := validItem
	missingNameItem.Name = ""
	runTestCase("Missing required 'name' field", missingNameItem, http.StatusBadRequest)
	fmt.Printf("\n--- Validation Test Summary: %d/%d tests passed. ---\n", passedCount, totalTests)
}

func runTrackingTest() {
	fmt.Println("--- Running Tracking API Endpoint Tests ---")
	passedCount := 0
	totalTests := 2
	fmt.Println("\n[SETUP] Fetching a valid line item to track...")
	resp, err := http.Get("http://localhost:8080/api/v1/lineitems")
	if err != nil || resp.StatusCode != http.StatusOK {
		fmt.Println("  [FAIL] Could not fetch line items to start the test. Is the service populated?")
		return
	}
	var items []LineItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil || len(items) == 0 {
		fmt.Println("  [FAIL] Could not decode or find any line items. Is the service populated?")
		resp.Body.Close()
		return
	}
	resp.Body.Close()
	itemToTrack := items[0]
	fmt.Printf("  - Using Line Item ID: %s\n", itemToTrack.ID)
	runTestCase := func(testName string, payload TrackingEvent, expectedStatus int) {
		fmt.Printf("\n[TEST] %s\n", testName)
		jsonData, _ := json.Marshal(payload)
		fmt.Printf("  - CURL Command: curl -X POST 'http://localhost:8080/api/v1/tracking' -H 'Content-Type: application/json' -d '%s'\n", string(jsonData))
		resp, err := http.Post("http://localhost:8080/api/v1/tracking", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Printf("  [RESULT] ❌ FAIL: API call failed: %v\n", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode == expectedStatus {
			fmt.Printf("  [RESULT] ✅ PASS: Received expected status %d\n", expectedStatus)
			passedCount++
		} else {
			body, _ := io.ReadAll(resp.Body)
			fmt.Printf("  [RESULT] ❌ FAIL: Expected status %d, but got %d. Body: %s\n", expectedStatus, resp.StatusCode, string(body))
		}
	}
	validEvent := TrackingEvent{
		EventType:  "impression",
		LineItemID: itemToTrack.ID,
		UserID:     "user-test-123",
		Placement:  itemToTrack.Placement,
		Metadata: map[string]string{
			"browser": "safari",
			"device":  "tablet",
		},
	}
	runTestCase("Sending a valid tracking event", validEvent, http.StatusAccepted)
	invalidEvent := TrackingEvent{
		EventType: "click",
	}
	runTestCase("Sending an invalid event (missing line_item_id)", invalidEvent, http.StatusBadRequest)
	fmt.Printf("\n--- Tracking Test Summary: %d/%d tests passed. ---\n", passedCount, totalTests)
}

func runE2ETrackingTest() {
	fmt.Println("--- Running End-to-End Tracking Pipeline Test ---")

	// --- 1. Get Initial State from ClickHouse ---
	fmt.Println("\n[PHASE 1] Getting initial row count from ClickHouse...")
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "mysecretpassword",
		},
	})
	if err != nil {
		fmt.Printf("  [FAIL] Could not connect to ClickHouse to get initial count: %v\n", err)
		return
	}

	var initialCount uint64
	queryInitial := "SELECT count() FROM default.ads_final"
	err = conn.QueryRow(context.Background(), queryInitial).Scan(&initialCount)
	if err != nil {
		fmt.Printf("  [FAIL] Failed to query ClickHouse for initial count: %v\n", err)
		conn.Close()
		return
	}
	conn.Close()
	fmt.Printf("  - Initial row count in ads_final is: %d\n", initialCount)

	// --- 2. Send Tracking Events ---
	fmt.Println("\n[PHASE 2] Sending tracking events to the API...")
	numEventsToSend := 15

	resp, err := http.Get("http://localhost:8080/api/v1/lineitems")
	if err != nil || resp.StatusCode != http.StatusOK {
		fmt.Println("  [FAIL] Could not fetch line items to start the test. Is the service populated?")
		return
	}
	var items []LineItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil || len(items) == 0 {
		fmt.Println("  [FAIL] Could not decode or find any line items.")
		resp.Body.Close()
		return
	}
	resp.Body.Close()
	itemToTrack := items[0]
	var sentCount int
	for sentCount = 0; sentCount < numEventsToSend; sentCount++ {
		event := TrackingEvent{
			EventType:  "impression",
			LineItemID: itemToTrack.ID,
			UserID:     fmt.Sprintf("e2e-user-%d", sentCount+1), // Make each user slightly different
			Placement:  itemToTrack.Placement,
			Metadata: map[string]string{
				"browser": "safari",
				"device":  "tablet",
			},
		}
		jsonData, _ := json.Marshal(event)

		// NEW: Print the curl command for this specific request
		fmt.Printf("\n--- Curl Request #%d ---\n", sentCount+1)
		fmt.Printf("curl -X POST 'http://localhost:8080/api/v1/tracking' -H 'Content-Type: application/json' -d '%s'\n", string(jsonData))

		resp, err := http.Post("http://localhost:8080/api/v1/tracking", "application/json", bytes.NewBuffer(jsonData))
		if err == nil && resp.StatusCode == http.StatusAccepted {
			spew.Dump(err.Error())
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Printf("\n  - Successfully sent %d tracking events.\n", sentCount)

	// --- 3. Wait for Pipeline Processing ---
	fmt.Println("\n[PHASE 3] Waiting 15 seconds for Kafka and ClickHouse to ingest the data...")
	time.Sleep(20 * time.Second)

	// --- 4. Query ClickHouse to Verify Final Count ---
	fmt.Println("\n[PHASE 4] Querying ClickHouse for final row count...")
	conn, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "mysecretpassword",
		},
	})
	if err != nil {
		fmt.Printf("  [FAIL] Could not connect to ClickHouse to get final count: %v\n", err)
		return
	}
	defer conn.Close()

	var finalCount uint64
	queryFinal := "SELECT count() FROM default.ads_final"
	err = conn.QueryRow(context.Background(), queryFinal).Scan(&finalCount)
	if err != nil {
		fmt.Printf("  [FAIL] Failed to query ClickHouse for final count: %v\n", err)
		return
	}
	fmt.Printf("  - Final row count in ads_final is: %d\n", finalCount)

	// --- 5. Compare and Report Result ---
	ingestedCount := finalCount - initialCount
	fmt.Println("\n[RESULT]")
	if sentCount == int(ingestedCount) {
		fmt.Printf("  ✅ PASS: Sent %d events. Row count correctly increased from %d to %d.\n", sentCount, initialCount, finalCount)
	} else {
		fmt.Printf("  ❌ FAIL: Mismatch! Sent %d events, but row count only increased by %d (from %d to %d).\n", sentCount, ingestedCount, initialCount, finalCount)
	}

	fmt.Println("\n--- Test Complete ---")
}

func printUsage() {
	fmt.Println("\nUsage: go run main.go <command> [arguments...]")
	fmt.Println("\nCommands:")
	fmt.Println("  create <name> <adv_id> <bid> <budget> <placement>")
	fmt.Println("  get-ad <p> <c> <k>  Get winning ad for a placement")
	fmt.Println("  ad-test           Run targeted tests for populated line items")
	fmt.Println("  validation-test   Test the line item creation validation rules")
	fmt.Println("  e2e-tracking-test Run an end-to-end tracking pipeline test")
	fmt.Println("\nExample:")
	fmt.Println(`  go run main.go ad-test`)
}
