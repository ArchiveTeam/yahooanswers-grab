dofile("urlcode.lua")
dofile("table_show.lua")

local url_count = 0
local tries = 0
local item_type = os.getenv('item_type')
local item_value = os.getenv('item_value')
local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')

local downloaded = {}
local addedtolist = {}

local questions = {}
local newquestions = {}
local newusers = {}

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

for question in string.gmatch(item_value, "([^,]+)") do
  questions[question] = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

allowed = function(url)
  if string.match(url, "qid=[a-z0-9A-Z]+") then
    newquestions[string.match(url, "qid=([a-z0-9A-Z]+)")] = true
  elseif string.match(url, "show=[a-z0-9A-Z]+") then
    newusers[string.match(url, "show=([a-z0-9A-Z]+)")] = true
  end
  if string.match(url, "^https?://[^/]*yimg%.com/hd/answers") then
    return true
  end
  for s in string.gmatch(url, "([a-z0-9A-Z]+)") do
    if questions[s] == true and string.match(url, "^https?://[^/]*answers%.yahoo%.com") then
      return true
    end
  end
  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if (downloaded[url] ~= true and addedtolist[url] ~= true)
      and (allowed(url) or html == 0)
      and not string.match(url, "https?://[^/]*answers%.yahoo%.com/question/index%?qid=[0-9a-zA-Z]+&a?m?p?;?page=[0-9]+") then
    addedtolist[url] = true
    return true
  else
    return false
  end
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil

  downloaded[url] = true
  
  local function check(urla)
    local url = string.match(urla, "^([^#]+)")
    if (downloaded[url] ~= true and addedtolist[url] ~= true) and allowed(url) then
      if string.match(url, "&amp;") then
        table.insert(urls, { url=string.gsub(url, "&amp;", "&") })
        addedtolist[url] = true
        addedtolist[string.gsub(url, "&amp;", "&")] = true
      else
        table.insert(urls, { url=url })
        addedtolist[url] = true
      end
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      if not string.match(newurl, "^https?://[^/]*answers%.yahoo%.com/question/index%?qid=[0-9a-zA-Z]+&a?m?p?;?page=[0-9]+") then
        check(newurl)
      end
    elseif string.match(newurl, "^https?:\\/\\/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      check(string.match(url, "^(https?:)")..string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(string.match(url, "^(https?:)")..newurl)
    elseif string.match(newurl, "^\\/") then
      check(string.match(url, "^(https?://[^/]+)")..string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(string.match(url, "^(https?://[^/]+)")..newurl)
    end
  end

  local function checknewshorturl(newurl)
    if string.match(newurl, "^%?") then
      check(string.match(url, "^(https?://[^%?]+)")..newurl)
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?") or string.match(newurl, "^[/\\]") or string.match(newurl, "^[jJ]ava[sS]cript:") or string.match(newurl, "^[mM]ail[tT]o:") or string.match(newurl, "^%${")) then
      check(string.match(url, "^(https?://.+/)")..newurl)
    end
  end
  
  if allowed(url) then
    html = read_file(file)
    for newurl in string.gmatch(html, '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, 'href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for p in string.gmatch(html, '<li[^>]+data%-ya%-type="answer"[^>]+>') do
      if string.match(p, 'data%-ya%-answer%-id="[^"]+"')
        and string.match(p, 'data%-ya%-question%-id="[^"]+"') then
        local answer_id = string.match(p, 'data%-ya%-answer%-id="([^"]+)"')
        local question_id = string.match(p, 'data%-ya%-question%-id="([^"]+)"')
        checknewurl('/_module?name=YAAnsCommentsModule&qid=' .. question_id .. '&aid=' .. string.gsub(answer_id, "=", "%%3D"))
      end
    end
  end

  return urls
end
  

wget.callbacks.httploop_result = function(url, err, http_stat)
  -- NEW for 2014: Slightly more verbose messages because people keep
  -- complaining that it's not moving or not working
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. ".  \n")
  io.stdout:flush()

  if (status_code >= 200 and status_code <= 399) then
    if string.match(url.url, "https://") then
      local newurl = string.gsub(url.url, "https://", "http://")
      downloaded[newurl] = true
    else
      downloaded[url.url] = true
    end
  end
  
  if status_code >= 500 or
    (status_code >= 400 and status_code ~= 404) or
    status_code == 0 then
    io.stdout:write("Server returned "..http_stat.statcode.." ("..err.."). Sleeping.\n")
    io.stdout:flush()
    os.execute("sleep 1")
    tries = tries + 1
    if tries >= 5 then
      io.stdout:write("\nI give up...\n")
      io.stdout:flush()
      tries = 0
      if allowed(url["url"]) then
        return wget.actions.ABORT
      else
        return wget.actions.EXIT
      end
    else
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0.7

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local file = io.open(item_dir..'/'..warc_file_base..'_data.txt', 'w')
  for newquestion, _ in pairs(newquestions) do
    file:write("question:" .. newquestion .. "\n")
  end
  for newuser, _ in pairs(newusers) do
    file:write("user:" .. newuser .. "\n")
  end
  file:close()
end
