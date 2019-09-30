module RailsEventStoreMongoid
  class EventRepository

    POSITION_SHIFT = 1
    SERIALIZED_GLOBAL_STREAM_NAME = "all".freeze

    attr_reader :adapter

    def initialize(adapter: ::RailsEventStoreMongoid::Event)
      @adapter = adapter
      @repo_reader = RailsEventStoreMongoid::EventRepositoryReader.new
    end

    def create(event, stream_name)
      adapter.create(
        stream:     stream_name,
        event_id:   event.event_id,
        event_type: event.class,
        data:       event.data,
        meta:       event.metadata,
      )
      event
    end

    def delete_stream(stream_name)
      condition = {stream: stream_name}
      adapter.destroy_all condition
    end

    def has_event?(event_id)
      adapter.where(event_id: event_id).exists?
    end

    def last_stream_event(stream_name)
      build_event_entity(adapter.where(stream: stream_name).desc(:ts).first)
    end

    def read_events_forward(stream_name, start_event_id, count)
      stream = adapter.where(stream: stream_name)
      unless start_event_id.equal?(:head)
        starting_event = adapter.find_by(event_id: start_event_id)
        stream = stream.where(:ts.gt => starting_event.ts)
      end

      stream.asc(:ts).limit(count)
        .map(&method(:build_event_entity))
    end

    def read_events_backward(stream_name, start_event_id, count)
      stream = adapter.where(stream: stream_name)
      unless start_event_id.equal?(:head)
        starting_event = adapter.find_by(event_id: start_event_id)
        stream = stream.where(:ts.lt => starting_event.ts)
      end

      stream.desc(:ts).limit(count)
        .map(&method(:build_event_entity))
    end

    def read_stream_events_forward(stream_name)
      adapter.where(stream: stream_name).asc(:ts)
        .map(&method(:build_event_entity))
    end

    def read_stream_events_backward(stream_name)
      adapter.where(stream: stream_name).desc(:ts)
        .map(&method(:build_event_entity))
    end

    def read_all_streams_forward(start_event_id, count)
      stream = adapter
      unless start_event_id.equal?(:head)
        starting_event = adapter.find_by(event_id: start_event_id)
        stream = stream.where(:ts.gt => starting_event.ts)
      end

      stream.asc(:ts).limit(count)
        .map(&method(:build_event_entity))
    end

    def read_all_streams_backward(start_event_id, count)
      stream = adapter
      unless start_event_id.equal?(:head)
        starting_event = adapter.find_by(event_id: start_event_id)
        stream = stream.where(:ts.lt => starting_event.ts)
      end

      stream.desc(:ts).limit(count)
        .map(&method(:build_event_entity))
    end

    def read(spec)
      @repo_reader.read(spec)
    end

    def count(specification)
      @repo_reader.count(specification)
    end

    def append_to_stream(events, stream, expected_version)
      add_to_stream(normalize_to_array(events), stream, expected_version, true)

      self
    end

    def link_to_stream(event_ids, stream, expected_version)
      normalized_event_ids = normalize_to_array(event_ids)
      found_events = []
      normalized_event_ids.each do |event_id|
        if event = Event.where(id: event_id).first
          found_events << build_event_instance(event)
        end
      end

      (normalized_event_ids - found_events.map(&:event_id)).each do |id|
        raise RubyEventStore::EventNotFound.new(id)
      end

      add_to_stream(found_events, stream, expected_version, nil)

      self
    end

    private

    def add_to_stream(collection, stream, expected_version, include_global)
      last_stream_version = -> (stream_) do
        Event
          .where(stream: stream_.name)
          .order_by(position: :desc)
          .first
          .try(:position)
      end
      resolved_version = expected_version.resolve_for(stream, last_stream_version)


      in_stream = collection.flat_map.with_index do |element, index|
        position = compute_position(resolved_version, index)

        collection = []

        if include_global
          collection << build_event_record_hash(element, SERIALIZED_GLOBAL_STREAM_NAME, nil)
        end

        unless stream.global?
          raise RubyEventStore::WrongExpectedEventVersion if expected_version_exists? stream.name, position
          raise RubyEventStore::EventDuplicatedInStream if adapter.has_duplicate?(element, stream.name, include_global)
          collection << build_event_record_hash(element, stream.name, position)
        end
        collection
      end

      Event.import(in_stream)

      self
    end

    def build_event_entity(record)
      return nil unless record
      record.event_type.constantize.new(
        event_id: record.event_id,
        metadata: record.meta,
        data: record.data,
      )
    end

    def build_event_record_hash(serialized_record, stream, position)
      {
        id:         serialized_record.event_id,
        stream:     stream,
        position:   position,
        data:       serialized_record.data,
        meta:       serialized_record.metadata,
        event_type: serialized_record.event_type
      }
    end

    def build_event_instance(mongoid_record)
      return nil unless mongoid_record
      RubyEventStore::SerializedRecord.new(
        event_id:   mongoid_record.id,
        metadata:   mongoid_record.meta,
        data:       mongoid_record.data,
        event_type: mongoid_record.event_type,
      )
    end

    def normalize_to_array(events)
      return events if events.is_a?(Enumerable)
      [events]
    end

    def compute_position(resolved_version, index)
      unless resolved_version.nil?
        resolved_version + index + POSITION_SHIFT
      end
    end
  end
end
