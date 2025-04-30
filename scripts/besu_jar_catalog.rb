#!/usr/bin/env ruby

require 'net/http'
require 'uri'
require 'nokogiri'
require 'fileutils'
require 'optparse'
require 'digest'
require 'timeout'

# Parse command line options
options = {
  limit: nil,
  pause: 500,  # Default pause in milliseconds
  cache_dir: File.join('/tmp', 'besu_jar_cache'),
  timeout: 30  # Default timeout in seconds
}

OptionParser.new do |opts|
  opts.banner = "Usage: besu_jar_catalog.rb [options]"
  opts.on("-l", "--limit NUMBER", Integer, "Limit the number of JARs to process") do |n|
    options[:limit] = n
  end
  opts.on("-p", "--pause MILLISECONDS", Integer, "Pause between downloads in milliseconds (default: 500ms)") do |p|
    options[:pause] = p
  end
  opts.on("-c", "--cache-dir DIRECTORY", "Directory to cache downloaded JARs (default: /tmp/besu_jar_cache)") do |dir|
    options[:cache_dir] = dir
  end
  opts.on("-t", "--timeout SECONDS", Integer, "Timeout for HTTP requests in seconds (default: 30s)") do |t|
    options[:timeout] = t
  end
  opts.on("-h", "--help", "Show this help message") do
    puts opts
    exit
  end
end.parse!

# Constants
BASE_URL = "https://hyperledger.jfrog.io/artifactory/besu-maven/org/hyperledger/besu/"
CATALOG_FILE = "besu_jar_catalog.txt"
TEMP_DIR = "temp_jars"
TARGET_VERSION = "25.4.0"
CACHE_DIR = options[:cache_dir]
PAUSE_MS = options[:pause]
TIMEOUT_SEC = options[:timeout]

# Create temp and cache directories
FileUtils.mkdir_p(TEMP_DIR)
FileUtils.mkdir_p(CACHE_DIR)

# Initialize catalog file
File.open(CATALOG_FILE, 'w') do |file|
  file.puts "HYPERLEDGER BESU JAR CATALOG"
  file.puts "Generated on: #{Time.now}"
  file.puts "=" * 80
end

# Function to fetch HTML content from URL with timeout
def fetch_url(url, timeout)
  puts "Fetching: #{url}"
  uri = URI.parse(url)

  begin
    Timeout.timeout(timeout) do
      response = Net::HTTP.get_response(uri)

      if response.code == "200"
        return response.body
      else
        puts "Error fetching #{url}: #{response.code}"
        return nil
      end
    end
  rescue Timeout::Error
    puts "Timeout fetching #{url}"
    return nil
  rescue => e
    puts "Exception fetching #{url}: #{e.message}"
    return nil
  end
end

# Function to parse directory listing
def parse_directory(html)
  doc = Nokogiri::HTML(html)
  links = doc.css('a').map { |link| link['href'] }.compact
  links.select { |link| link != "../" }
end

# Function to download JAR file with caching and timeout
def download_jar(url, output_path, cache_dir, pause_ms, timeout)
  # Create a cache key based on the URL
  cache_key = Digest::MD5.hexdigest(url)
  cache_path = File.join(cache_dir, "#{cache_key}.jar")

  # Check if the JAR is already in cache
  if File.exist?(cache_path)
    puts "Using cached version of #{url}"
    FileUtils.cp(cache_path, output_path)
    return true
  end

  # Download the JAR
  puts "Downloading: #{url}"
  uri = URI.parse(url)

  begin
    Timeout.timeout(timeout) do
      Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') do |http|
        request = Net::HTTP::Get.new(uri)

        http.request(request) do |response|
          if response.code == "200"
            File.open(output_path, 'wb') do |file|
              response.read_body do |chunk|
                file.write(chunk)
              end
            end

            # Cache the downloaded JAR
            FileUtils.cp(output_path, cache_path)

            # Pause after download to avoid hammering the server
            puts "Pausing for #{pause_ms}ms after download..."
            sleep(pause_ms / 1000.0)

            return true
          else
            puts "Error downloading #{url}: #{response.code}"
            return false
          end
        end
      end
    end
  rescue Timeout::Error
    puts "Timeout downloading #{url}"
    return false
  rescue => e
    puts "Exception downloading #{url}: #{e.message}"
    return false
  end
end

# Function to extract Java classes from JAR
def extract_classes(jar_path)
  output = `jar tf #{jar_path} | grep "\.class$" | sort`
  output.split("\n").map { |line| line.gsub(/\$.*\.class$/, '.class') }.uniq
end

# Function to append JAR info to catalog
def append_to_catalog(jar_name, module_name, version, classes)
  File.open(CATALOG_FILE, 'a') do |file|
    file.puts "\nJAR: #{jar_name}"
    file.puts "MODULE: #{module_name}"
    file.puts "VERSION: #{version}"
    file.puts "CLASSES:"
    classes.each do |class_name|
      file.puts "  #{class_name}"
    end
    file.puts "-" * 80
  end
end

# Simplified main function - just process the main modules
def process_besu_modules(base_url, limit, cache_dir, pause_ms, timeout)
  jars_processed = 0

  # Get the main modules
  html = fetch_url(base_url, timeout)
  return 0 unless html

  modules = parse_directory(html)

  # Also add the internal directory
  internal_url = "#{base_url}internal/"
  internal_html = fetch_url(internal_url, timeout)
  if internal_html
    internal_modules = parse_directory(internal_html).map { |m| "internal/#{m}" }
    modules.concat(internal_modules)
  end

  # Process each module
  modules.each do |module_path|
    break if limit && jars_processed >= limit

    next unless module_path.end_with?('/')

    module_name = module_path.chomp('/')
    module_url = "#{base_url}#{module_path}"

    # Get versions
    versions_html = fetch_url(module_url, timeout)
    next unless versions_html

    versions = parse_directory(versions_html)

    # Filter out non-version directories and SNAPSHOT versions
    version_dirs = versions.select { |v| v.end_with?('/') && v =~ /^\d+(\.\d+)*\/$/ && !v.include?("SNAPSHOT") }

    # Select target version or latest
    selected_version = nil
    if version_dirs.include?("#{TARGET_VERSION}/")
      selected_version = "#{TARGET_VERSION}/"
    else
      selected_version = version_dirs.sort_by { |v| v.gsub(/[^\d.]/, '').split('.').map(&:to_i) }.last
    end

    next unless selected_version

    version = selected_version.chomp('/')
    version_url = "#{module_url}#{selected_version}"

    # Find JAR file
    jar_html = fetch_url(version_url, timeout)
    next unless jar_html

    jar_files = parse_directory(jar_html).select { |f| f.end_with?(".jar") && !f.include?("-sources") && !f.include?("-javadoc") }
    jar_file = jar_files.find { |f| f == "#{module_name.split('/').last}-#{version}.jar" } || jar_files.first
    next unless jar_file

    jar_url = "#{version_url}#{jar_file}"
    jar_path = File.join(TEMP_DIR, jar_file)

    puts "Processing #{jar_file} (#{module_name} #{version})..."
    if download_jar(jar_url, jar_path, cache_dir, pause_ms, timeout)
      puts "Extracting classes from #{jar_file}..."
      classes = extract_classes(jar_path)

      puts "Adding to catalog: #{jar_file}"
      append_to_catalog(jar_file, module_name, version, classes)

      jars_processed += 1
    end
  end

  return jars_processed
end

# Main execution
begin
  puts "Starting Besu JAR catalog generation..."
  puts "Using pause of #{PAUSE_MS}ms between downloads"
  puts "Caching JARs in #{CACHE_DIR}"
  puts "HTTP timeout: #{TIMEOUT_SEC} seconds"

  jars_processed = process_besu_modules(BASE_URL, options[:limit], CACHE_DIR, PAUSE_MS, TIMEOUT_SEC)
  puts "Processed #{jars_processed} JARs"
  puts "Catalog generated: #{CATALOG_FILE}"

  # Clean up temp directory
  FileUtils.rm_rf(TEMP_DIR)
rescue => e
  puts "Error: #{e.message}"
  puts e.backtrace
ensure
  # Make sure we clean up even on error
  FileUtils.rm_rf(TEMP_DIR) if Dir.exist?(TEMP_DIR)
end
