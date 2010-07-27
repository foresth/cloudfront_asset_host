require 'right_aws'
require 'tempfile'

module CloudfrontAssetHost
  module Uploader
    
    mattr_accessor :new_keys

    class << self

      def upload!(options = {})
        dryrun = options.delete(:dryrun) || false
        verbose = options.delete(:verbose) || false

        self.new_keys = []

        puts "-- Updating uncompressed files" if verbose
        upload_keys_with_paths(keys_with_paths, dryrun, verbose, false)

        if CloudfrontAssetHost.gzip
          puts "-- Updating compressed files" if verbose
          upload_keys_with_paths(gzip_keys_with_paths, dryrun, verbose, true)
        end
        
        delete_old_keys(dryrun, verbose)

        @existing_keys = nil
      end

      def upload_keys_with_paths(keys_paths, dryrun, verbose, gzip)
        keys_paths.each do |key, path|
          new_keys << key
          if !existing_keys.include?(key) || CloudfrontAssetHost.css?(path) && rewrite_all_css?
            puts "+ #{key}" if verbose

            extension = File.extname(path)[1..-1]

            path = rewritten_css_path(path)

            data_path = gzip ? gzipped_path(path) : path
            bucket.put(key, File.read(data_path), {}, 'public-read', headers_for_path(extension, gzip)) unless dryrun

            File.unlink(data_path) if gzip && File.exists?(data_path)
          else
            puts "= #{key}" if verbose
          end
        end
      end

      def gzipped_path(path)
        tmp = Tempfile.new("cfah-gz")
        `gzip #{path} -q -c > #{tmp.path}`
        tmp.path
      end

      def rewritten_css_path(path)
        if CloudfrontAssetHost.css?(path)
          tmp = CloudfrontAssetHost::CssRewriter.rewrite_stylesheet(path)
          tmp.path
        else
          path
        end
      end

      def keys_with_paths
        current_paths.inject({}) do |result, path|
          key = CloudfrontAssetHost.key_for_path(path) + path.gsub(Rails.public_path, '')

          result[key] = path
          result
        end
      end

      def gzip_keys_with_paths
        current_paths.inject({}) do |result, path|
          source = path.gsub(Rails.public_path, '')

          if CloudfrontAssetHost.gzip_allowed_for_source?(source)
            key = "#{CloudfrontAssetHost.gzip_prefix}/" << CloudfrontAssetHost.key_for_path(path) << source
            result[key] = path
          end

          result
        end
      end
      
      def delete_old_keys(dryrun, verbose)
        puts "-- Removing expired files" if verbose
        (existing_keys - new_keys).uniq.each do |key|
          unless new_keys.include?(key)
            puts "- #{key}" if verbose
            bucket.delete_folder(key) unless dryrun
          end
        end
      end
      
      def rewrite_all_css?
        @rewrite_all_css ||= !keys_with_paths.delete_if { |key, path| existing_keys.include?(key) || !CloudfrontAssetHost.image?(path) }.empty?
      end

      def existing_keys
        @existing_keys ||= begin
          keys = []
          keys.concat bucket.keys('prefix' => CloudfrontAssetHost.key_prefix).map  { |key| key.name }
          keys.concat bucket.keys('prefix' => CloudfrontAssetHost.gzip_prefix).map { |key| key.name }
          keys
        end
      end

      def current_paths
        @current_paths ||= Dir.glob("#{Rails.public_path}/{images,javascripts,stylesheets}/**/*").reject { |path| File.directory?(path) }
      end

      def headers_for_path(extension, gzip = false)
        mime = ext_to_mime[extension] || 'application/octet-stream'
        headers = {
          'Content-Type' => mime,
          'Cache-Control' => "max-age=#{10.years.to_i}",
          'Expires' => 1.year.from_now.utc.to_s
        }
        headers['Content-Encoding'] = 'gzip' if gzip

        headers
      end

      def ext_to_mime
        @ext_to_mime ||= Hash[ *( YAML::load_file(File.join(File.dirname(__FILE__), "mime_types.yml")).collect { |k,vv| vv.collect{ |v| [v,k] } }.flatten ) ]
      end

      def bucket
        @bucket ||= begin 
          bucket = s3.bucket(CloudfrontAssetHost.bucket)
          bucket.disable_logging unless CloudfrontAssetHost.s3_logging
          bucket
        end
      end

      def s3
        @s3 ||= RightAws::S3.new(config['access_key_id'], config['secret_access_key'])
      end

      def config
        @config ||= YAML::load_file(CloudfrontAssetHost.s3_config)
      end

    end

  end
end