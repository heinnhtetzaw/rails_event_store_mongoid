require 'spec_helper'
require 'ruby_event_store'
require 'event_repository_lint'

describe RailsEventStoreMongoid::EventRepository do

  # https://github.com/arkency/ruby_event_store/pull/31
  it_behaves_like :event_repository, described_class
  let(:mapper)                     { RubyEventStore::Mappers::NullMapper.new }
  let(:repository)                 { described_class.new }
  let(:reader)                     { RubyEventStore::SpecificationReader.new(repository, mapper) }
  let(:specification)              { RubyEventStore::Specification.new(reader) }
  let(:stream)                     { RubyEventStore::Stream.new('stream') }
  let(:test_race_conditions_any ) { false }
  let(:test_race_conditions_auto ) { false }
  let(:test_binary) { false }
  let(:test_change) { false }

  specify 'initialize with adapter' do
    repository = described_class.new
    expect(repository.adapter).to eq(RailsEventStoreMongoid::Event)
  end

  specify 'provide own event implementation' do
    CustomEvent = Class.new
    repository = described_class.new(adapter: CustomEvent)
    expect(repository.adapter).to eq(CustomEvent)
  end

  describe 'event ordering' do
    let(:stream_name)             { 'test_stream' }
    let(:test_stream) { RubyEventStore::Stream.new('test_stream') }

    class SimpleEvent
      attr_reader :event_id
      def initialize(event_id: SecureRandom.uuid, metadata: nil, data: nil)
        @event_id = event_id.to_i
      end
    end

    def create_event(event_id:, stream: test_stream)
      internal = { event_type: 'SimpleEvent', stream: stream.name }
      RailsEventStoreMongoid::Event.create!(
                       internal.merge(event_id: event_id, id: event_id)
      )
    end

    before do
      create_event(event_id: 2)
      create_event(event_id: 1)

      create_event(event_id: 20, stream: RubyEventStore::Stream.new('other_stream'))
      create_event(event_id: 5, stream: stream)

      create_event(event_id: 4)
      create_event(event_id: 3)

    end

    specify '#last_stream_event' do
      expect(subject.last_stream_event(stream).event_id.to_i).to eq(5)
    end

    specify '#read_events_forward' do
      expect(subject.read_events_forward(stream_name, 1, 5).map(&:event_id)).to eq([4,3])
    end

    specify '#read_events_backward' do
      expect(subject.read_events_backward(stream_name, 4, 5).map(&:event_id)).to eq([1,2])
    end

    specify '#read_stream_events_forward' do
      expect(subject.read_stream_events_forward(stream_name).map(&:event_id)).to eq([2,1,4,3])
    end

    specify '#read_stream_events_backward' do
      expect(subject.read_stream_events_backward(stream_name).map(&:event_id)).to eq([3,4,1,2])
    end

    specify '#read_all_streams_backward' do
      expect(subject.read_all_streams_backward(4, 5).map(&:event_id)).to eq([5,20,1,2])
    end

    specify '#read_all_streams_forward' do
      expect(subject.read_all_streams_forward(1, 5).map(&:event_id)).to eq([20,5,4,3])
    end

    specify '#delete_stream' do
      subject.delete_stream(RubyEventStore::Stream.new('other_stream'))
      expect(subject.read_events_forward(stream_name, 2, 5).map(&:event_id)).to eq([1,4,3])
    end
  end

  specify "all considered internal detail" do
    repository.append_to_stream(
      [RubyEventStore::SRecord.new],
      RubyEventStore::Stream.new(RubyEventStore::GLOBAL_STREAM),
      RubyEventStore::ExpectedVersion.any
    )

    expect do
      repository.read(specification.stream("all").result)
    end.to raise_error(RubyEventStore::ReservedInternalName)

    expect do
      repository.read(specification.stream("all").backward.result)
    end.to raise_error(RubyEventStore::ReservedInternalName)

    expect do
      repository.read(specification.stream("all").limit(5).result)
    end.to raise_error(RubyEventStore::ReservedInternalName)

    expect do
      repository.read(specification.stream("all").limit(5).backward.result)
    end.to raise_error(RubyEventStore::ReservedInternalName)

    expect do
      repository.count(specification.stream("all").result)
    end.to raise_error(RubyEventStore::ReservedInternalName)
  end
end
