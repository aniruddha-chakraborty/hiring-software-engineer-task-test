-- tracking_load.lua

-- This function is called once per thread to set up the static parts of the request.
wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"
wrk.body   = [[
  {
    "event_type": "conversion",
    "line_item_id": "ad_77777",
    "timestamp": "2025-07-22T22:10:15Z",
    "placement": "video_preroll",
    "user_id": "user_99999",
    "metadata": {
      "browser": "safari",
      "device": "tablet"
    }
  }
]]

-- This function is called for every request. Since the body is static,
-- we just need to return the path.
request = function()
  return wrk.format(nil, "/api/v1/tracking")
end