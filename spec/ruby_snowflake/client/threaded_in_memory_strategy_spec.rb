# frozen_string_literal: true

require "rb_snowflake_client"

RSpec.describe RubySnowflake::Client::ThreadedInMemoryStrategy do
  let(:row_types) do
    [{"name" => "value", "type" => "text", "scale" => 0, "precision" => 0}]
  end
  let(:statement_json_body) do
    {
      "resultSetMetaData" => {"partitionInfo" => [{}, {}, {}], "rowType" => row_types},
      "data" => [["first"]],
    }
  end
  let(:thread_pool) { Concurrent::FixedThreadPool.new(2) }

  before do
    allow(Concurrent::FixedThreadPool).to receive(:new).with(2).and_return(thread_pool)
    allow(thread_pool).to receive(:shutdown).and_call_original
    allow(thread_pool).to receive(:wait_for_termination).and_call_original
  end

  def fetch(&retrieve_proc)
    described_class.result(statement_json_body, retrieve_proc, 2)
  end

  it "assembles partitions in order and shuts down its pool" do
    result = fetch { |index| [["partition #{index}"]] }

    expect(result.map { |row| row["value"] }).to eq(["first", "partition 1", "partition 2"])
    expect(thread_pool).to have_received(:shutdown)
    expect(thread_pool).to have_received(:wait_for_termination)
    expect(thread_pool).to be_shutdown
  end

  it "propagates RubySnowflake errors from slow partition fetches and shuts down its pool" do
    expect do
      fetch do |index|
        sleep 0.05 # still pending when the result loop reaches it
        raise RubySnowflake::BadResponseError, "partition #{index} failed" if index == 1
        [["partition #{index}"]]
      end
    end.to raise_error(RubySnowflake::BadResponseError, "partition 1 failed")

    expect(thread_pool).to have_received(:shutdown)
    expect(thread_pool).to have_received(:wait_for_termination)
  end

  it "wraps other partition fetch failures in ConnectionStarvedError" do
    expect do
      fetch do |index|
        sleep 0.05
        raise "boom" if index == 2
        [["partition #{index}"]]
      end
    end.to raise_error(RubySnowflake::ConnectionStarvedError, /Rejection reason: boom/)

    expect(thread_pool).to have_received(:shutdown)
  end
end
