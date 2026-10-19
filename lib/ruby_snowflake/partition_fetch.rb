# frozen_string_literal: true

module RubySnowflake
  # Shared by the threaded and streaming result paths, which fetch partitions
  # in Concurrent::Futures.
  module PartitionFetch
    CONNECTION_STARVED_MESSAGE =
      "Timed out waiting for a connection to fetch a result partition. This is usually due to using " \
      "the client in multiple threads. The client uses a connection pool and if too many requests " \
      "are all done in threads at the same time, threads can get starved of access to connections. " \
      "The solution for this is to either increase the max_connections parameter on the client or " \
      "create a new client instance with its own connection pool to snowflake per thread."

    # Blocks until the future resolves and returns its partition data, or
    # raises a RubySnowflake::Error for the failed fetch. Errors that aren't
    # already RubySnowflake errors are wrapped, keeping the original as `cause`.
    def self.value!(future)
      future.wait
      return future.value unless future.rejected?

      reason = future.reason
      case reason
      when RubySnowflake::Error
        raise reason
      when ConnectionPool::TimeoutError
        raise ConnectionStarvedError.new("#{CONNECTION_STARVED_MESSAGE} Rejection reason: #{reason.message}"), cause: reason
      else
        raise Error.new("Fetching a result partition failed: #{reason.class}: #{reason.message}"), cause: reason
      end
    end
  end
end
