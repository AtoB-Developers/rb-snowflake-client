# frozen_string_literal: true

require "spec_helper"

RSpec.describe RubySnowflake::StreamingResult do
  let(:row_types) do
    [{"name" => "value", "type" => "text", "scale" => 0, "precision" => 0}]
  end
  let(:pools) { [] }
  let(:thread_pool) { pools.first }

  before do
    allow(Concurrent::FixedThreadPool).to receive(:new).and_wrap_original do |original, *args, **opts|
      original.call(*args, **opts).tap do |pool|
        allow(pool).to receive(:shutdown).and_call_original
        allow(pool).to receive(:wait_for_termination).and_call_original
        pools << pool
      end
    end
  end

  def build_result(&retrieve_proc)
    result = described_class.new(2, row_types, retrieve_proc)
    result[0] = [["first"]]
    result
  end

  it "shuts down its prefetch pool after iteration" do
    values = build_result { [["second"]] }.map { |row| row["value"] }

    expect(values).to eq(%w[first second])
    expect(thread_pool).to have_received(:shutdown)
    expect(thread_pool).to have_received(:wait_for_termination)
  end

  it "shuts down its prefetch pool when the consumer stops early" do
    build_result { [["second"]] }.each { break }

    expect(thread_pool).to have_received(:shutdown)
    expect(thread_pool).to have_received(:wait_for_termination)
  end

  it "shuts down its prefetch pool when the consumer raises" do
    expect do
      build_result { [["second"]] }.each { raise "consumer failed" }
    end.to raise_error("consumer failed")

    expect(thread_pool).to have_received(:shutdown)
    expect(thread_pool).to have_received(:wait_for_termination)
  end

  it "propagates prefetch failures as RubySnowflake::Error and shuts down the pool" do
    expect do
      build_result { raise "partition failed" }.to_a
    end.to raise_error(RubySnowflake::Error, /RuntimeError: partition failed/) { |error|
      expect(error.cause).to be_a(RuntimeError)
    }

    expect(thread_pool).to have_received(:shutdown)
    expect(thread_pool).to have_received(:wait_for_termination)
  end

  it "re-raises RubySnowflake errors from prefetches unchanged" do
    expect do
      build_result { raise RubySnowflake::BadResponseError, "500 from Snowflake" }.to_a
    end.to raise_error(RubySnowflake::BadResponseError, "500 from Snowflake")
  end

  it "wraps connection pool timeouts in ConnectionStarvedError" do
    expect do
      build_result { raise ConnectionPool::TimeoutError, "waited 60 sec" }.to_a
    end.to raise_error(RubySnowflake::ConnectionStarvedError, /Rejection reason: waited 60 sec/)
  end
end
