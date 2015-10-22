require 'uri'
require 'puppet/network/http_pool'
require 'net/http'
require 'timeout'
require 'pp'
require 'thread'

module Puppet::Util::Puppetdb
  class Http

    SERVER_URL_FAIL_MSG = "Failing over to the next PuppetDB server_url in the 'server_urls' list"

    # Concat two server_url snippets, taking into account a trailing/leading slash to
    # ensure a correct server_url is constructed
    #
    # @param snippet1 [String] first URL snippet
    # @param snippet2 [String] second URL snippet
    # @return [String] returns http response
    # @api private
    def self.concat_url_snippets(snippet1, snippet2)
      if snippet1.end_with?('/') and snippet2.start_with?('/')
        snippet1 + snippet2[1..-1]
      elsif !snippet1.end_with?('/') and !snippet2.start_with?('/')
        snippet1 + '/' + snippet2
      else
        snippet1 + snippet2
      end
    end

    # Run the given block (cb) in a begin/rescue, catching common network
    # exceptions and logging useful information about them. If an expected
    # exception was caught, it's returned. An unexpected exception will be
    # re-thrown. Returns nil on success.
    def self.with_http_error_logging(server_url, route, &cb)
      config = Puppet::Util::Puppetdb.config
      server_url_config = config.server_url_config?

      begin
        cb.call()
      rescue Timeout::Error => e
        Puppet.warning("Request to #{server_url.host} on #{server_url.port} at route #{route} timed out " \
          "after #{config.server_url_timeout} seconds. #{SERVER_URL_FAIL_MSG if server_url_config}")
        return e

      rescue SocketError, OpenSSL::SSL::SSLError, SystemCallError, Net::ProtocolError, IOError, Net::HTTPNotFound => e
        Puppet.warning("Error connecting to #{server_url.host} on #{server_url.port} at route #{route}, " \
          "error message received was '#{e.message}'. #{SERVER_URL_FAIL_MSG if server_url_config}")
        return e

      rescue Puppet::Util::Puppetdb::InventorySearchError => e
        Puppet.warning("Could not perform inventory search from PuppetDB at #{server_url.host}:#{server_url.port}: " \
          "'#{e.message}' #{SERVER_URL_FAIL_MSG if server_url_config}")
        return e

      rescue Puppet::Util::Puppetdb::CommandSubmissionError => e
        error = "Failed to submit '#{e.context[:command]}' command for '#{e.context[:for_whom]}' to PuppetDB " \
          "at #{server_url.host}:#{server_url.port}: '#{e.message}'."
        if config.soft_write_failure
          Puppet.err error
        else
          Puppet.warning(error + " #{SERVER_URL_FAIL_MSG if server_url_config}")
        end
        return e

      rescue Puppet::Util::Puppetdb::SoftWriteFailError => e
        Puppet.warning("Failed to submit '#{e.context[:command]}' command for '#{e.context[:for_whom]}' to PuppetDB " \
          "at #{server_url.host}:#{server_url.port}: '#{e.message}' #{SERVER_URL_FAIL_MSG if server_url_config}")
        return e

      rescue Puppet::Error => e
        if e.message =~ /did not match server certificate; expected one of/
          Puppet.warning("Error connecting to #{server_url.host} on #{server_url.port} at route #{route}, " \
            "error message received was '#{e.message}'. #{SERVER_URL_FAIL_MSG if server_url_config}")
          return e
        else
          raise
        end
      end

      nil
    end

    # Check an http reponse from puppetdb; log a useful message if it looks like
    # something went wrong. Return a symbol indicating the problem
    # (:server_error, :notfound, or :other_404), or nil if there wasn't one.
    def self.check_http_response(response, server_url, route)
      server_url_config = Puppet::Util::Puppetdb.config.server_url_config?

      if response.is_a? Net::HTTPServerError
        Puppet.warning("Error connecting to #{server_url.host} on #{server_url.port} at route #{route}, " \
          "error message received was '#{response.message}'. #{SERVER_URL_FAIL_MSG if server_url_config}")
        :server_error
      elsif response.is_a? Net::HTTPNotFound
        if response.body && response.body.chars.first == "{"
          # If it appears to be json, we've probably gotten an authentic 'not found' message.
          Puppet.debug("HTTP 404 (probably normal) when connecting to #{server_url.host} on #{server_url.port} " \
            "at route #{route}, error message received was '#{response.message}'. #{SERVER_URL_FAIL_MSG if server_url_config}")
          :notfound
        else
          # But we can also get 404s when conneting to a puppetdb that's still starting or due to misconfiguration.
          Puppet.warning("Error connecting to #{server_url.host} on #{server_url.port} at route #{route}, " \
            "error message received was '#{response.message}'. #{SERVER_URL_FAIL_MSG if server_url_config}")
          :other_404
        end
      else
        nil
      end
    end

    def self.raise_request_error(response, response_error, path_suffix)
      if Puppet::Util::Puppetdb.config.server_url_config?
        server_url_strings = Puppet::Util::Puppetdb.config.server_urls.map {|server_url| server_url.to_s}.join(', ')
        if response_error == :notfound
          raise NotFoundError, "Failed to find '#{path_suffix}' on any of the following 'server_urls': #{server_url_strings}"
        else
          raise Puppet::Error, "Failed to execute '#{path_suffix}' on any of the following 'server_urls': #{server_url_strings}"
        end
      else
        uri = Puppet::Util::Puppetdb.config.server_urls.first
        if response_error == :notfound
          raise NotFoundError, "Failed to find '#{path_suffix}' on server: '#{uri.host}' and port: '#{uri.port}'"
        else
          raise Puppet::Error, "Failed to execute '#{path_suffix}' on server: '#{uri.host}' and port: '#{uri.port}'"
        end
      end
    end

    # Setup an http connection, provide a block that will do something with that http
    # connection. The block should be a two argument block, accepting the connection (which
    # you can call get or post on for example) and the properly constructed path, which
    # will be the concatenated version of any url_prefix and the path passed in.
    #
    # @param path_suffix [String] path for the get/post of the http action
    # @param request_type [Symbol] :query or :command
    # @param http_callback [Proc] proc containing the code calling the action on the http connection
    # @return [Response] returns http response
    def self.action(path_suffix, request_mode, &http_callback)
      response = nil
      response_error = nil
      config = Puppet::Util::Puppetdb.config

      for server_url in config.server_urls
        route = concat_url_snippets(server_url.request_uri, path_suffix)

        request_exception = with_http_error_logging(server_url, route) {
          http = Puppet::Network::HttpPool.http_instance(server_url.host, server_url.port)

          response = timeout(config.server_url_timeout) do
            http_callback.call(http, route)
          end
        }

        if request_exception.nil?
          response_error = check_http_response(response, server_url, route)
          if response_error.nil?
            break
          end
        end
      end

      if response.nil? or not(response_error.nil?)
        raise_request_error(response, response_error, path_suffix)
      end

      response
    end
  end

  class NotFoundError < Puppet::Error
  end
end
