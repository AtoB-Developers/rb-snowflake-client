# frozen_string_literal: true

require "spec_helper"

RSpec.describe RubySnowflake::Client::ThreadedInMemoryStrategy do
  let(:row_types) do
    [{"name" => "value", "type" => "text", "scale" => 0, "precision" => 0}]
  end
  let(:partition_count) { 3 }
  let(:statement_json_body) do
    {
      "resultSetMetaData" => {"partitionInfo" => Array.new(partition_count) { {} }, "rowType" => row_types},
      "data" => [["first"]],
    }
  end
  let(:pools) { [] }
  let(:fetched) { Concurrent::Array.new }

  before do
    allow(Concurrent::FixedThreadPool).to receive(:new).and_wrap_original do |original, *args, **opts|
      original.call(*args, **opts).tap { |pool| pools << pool }
    end
  end

  def fetch(&retrieve_proc)
    described_class.result(statement_json_body, ->(index) { fetched << index; retrieve_proc.call(index) }, 2)
  end

  def elapsed
    started = Concurrent.monotonic_time
    yield
  ensure
    return Concurrent.monotonic_time - started
  end

  it "assembles partitions in order and shuts down its pool" do
    result = fetch { |index| [["partition #{index}"]] }

    expect(result.map { |row| row["value"] }).to eq(["first", "partition 1", "partition 2"])
    expect(pools.size).to eq(1)
    expect(pools.first).not_to be_running
  end

  it "raises failures from fetches still pending when the result loop reaches them" do
    expect do
      fetch do |index|
        sleep 0.05
        raise RubySnowflake::BadResponseError, "partition #{index} failed" if index == 1
        [["partition #{index}"]]
      end
    end.to raise_error(RubySnowflake::BadResponseError, "partition 1 failed")

    expect(pools.first).not_to be_running
  end

  context "with many partitions" do
    let(:partition_count) { 12 }

    it "fails fast without running the remaining fetches" do
      duration = elapsed do
        expect do
          fetch do |index|
            raise RubySnowflake::BadResponseError, "partition 1 failed" if index == 1
            sleep 0.2
            [["partition #{index}"]]
          end
        end.to raise_error(RubySnowflake::BadResponseError)
      end

      expect(duration).to be < 0.15
      sleep 0.3 # let the in-flight fetch finish
      expect(fetched.size).to be <= 3 # partition 1 plus at most one in flight per worker
      expect(pools.first).not_to be_running
    end

    it "lets an interrupt abort the query without waiting for the remaining fetches" do
      duration = elapsed do
        expect do
          Timeout.timeout(0.1) { fetch { |index| sleep 0.3; [["partition #{index}"]] } }
        end.to raise_error(Timeout::Error)
      end

      expect(duration).to be < 0.25
      expect(pools.first).not_to be_running
    end
  end

  it "wraps connection pool timeouts in ConnectionStarvedError" do
    expect do
      fetch { |index| raise ConnectionPool::TimeoutError, "waited 60 sec" if index == 2; [["partition #{index}"]] }
    end.to raise_error(RubySnowflake::ConnectionStarvedError, /Rejection reason: waited 60 sec/) { |error|
      expect(error.cause).to be_a(ConnectionPool::TimeoutError)
    }
  end

  it "wraps other failures in RubySnowflake::Error, keeping the original as the cause" do
    expect do
      fetch { |index| raise JSON::ParserError, "unexpected token" if index == 2; [["partition #{index}"]] }
    end.to raise_error(RubySnowflake::Error, /JSON::ParserError: unexpected token/) { |error|
      expect(error).not_to be_a(RubySnowflake::ConnectionStarvedError)
      expect(error.cause).to be_a(JSON::ParserError)
    }
  end
end
