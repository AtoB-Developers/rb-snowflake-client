# frozen_string_literal: true

module RubySnowflake
  class Client
    class ThreadedInMemoryStrategy
      def self.result(statement_json_body, retreive_proc, num_threads)
        partitions = statement_json_body["resultSetMetaData"]["partitionInfo"]
        result = Result.new(partitions.size, statement_json_body["resultSetMetaData"]["rowType"])
        result[0] = statement_json_body["data"]

        thread_pool = Concurrent::FixedThreadPool.new(num_threads)
        futures = []
        begin
          partitions.each_index do |index|
            next if index == 0 # already have the first partition
            futures << [index, Concurrent::Future.execute(executor: thread_pool) { retreive_proc.call(index) }]
          end

          futures.each do |index, future|
            result[index] = PartitionFetch.value!(future)
          end
        ensure
          # FixedThreadPool workers never idle out, so without an explicit
          # shutdown every multi-partition query strands num_threads threads
          # for the life of the process. On success every future has already
          # resolved. On a failed fetch or an interrupt (Timeout, Thread#raise),
          # cancel the fetches that haven't started and don't wait for the
          # in-flight ones, so the error reaches the caller right away; the
          # workers exit once their current fetch finishes.
          futures.each { |_index, future| future.cancel }
          thread_pool.shutdown
        end
        result
      end
    end
  end
end
