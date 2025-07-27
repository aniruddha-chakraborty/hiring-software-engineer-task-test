-- random_ads.lua

-- Define the lists of possible parameters
local placements = {"homepage_top", "video_preroll", "article_inline_1"}
local categories = {"electronics", "fashion", "beauty", "travel", "food", "gaming", "home", "sports"}
local keywords = {"summer", "discount", "clearance", "deal", "exclusive", "trending", "new", "sale"}

-- This function is called by wrk for each request
request = function()
    -- Randomly select one parameter from each list
    local p = placements[math.random(#placements)]
    local c = categories[math.random(#categories)]
    local k = keywords[math.random(#keywords)]

    -- Construct the path with the random query parameters
    local path = string.format("/api/v1/ads?placement=%s&category=%s&keyword=%s&limit=4", p, c, k)

    -- Return the request object
    return wrk.format("GET", path)
end