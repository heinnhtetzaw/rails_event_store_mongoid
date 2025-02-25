module RailsEventStoreMongoid
  class EventRepositoryReader
    def has_event?(event_id)
      Event.where(id: event_id).exists?
    end

    def last_stream_event(stream)
      record = Event.where(stream: stream.name).order_by(position: :desc, id: :desc).first
      record && build_event_instance(record)
    end

    def read(spec)
      raise RubyEventStore::ReservedInternalName if spec.stream.name.eql?(EventRepository::SERIALIZED_GLOBAL_STREAM_NAME)

      stream = read_scope(spec)

      if spec.batched?
        batch_reader = ->(offset, limit) do
          stream.skip(offset).limit(limit).map(&method(:build_event_instance))
        end
        RubyEventStore::BatchEnumerator.new(spec.batch_size, spec.limit, batch_reader).each
      elsif spec.first?
        record = stream.first
        build_event_instance(record) if record
      elsif spec.last?
        record = stream.last
        build_event_instance(record) if record
      else
        stream.map(&method(:build_event_instance)).each
      end
    end

    def count(spec)
      raise RubyEventStore::ReservedInternalName if spec.stream.name.eql?(EventRepository::SERIALIZED_GLOBAL_STREAM_NAME)

      Array(read_scope(spec)).count
    end

    private

    def read_scope(spec)
      stream = Event.where(stream: normalize_stream_name(spec))
      stream = stream.where(event_id: { '$in': spec.with_ids }) if spec.with_ids?
      stream = stream.where(event_type: { '$in': spec.with_types }) if spec.with_types?
      stream = stream.order_by(position: order(spec)) unless spec.stream.global?
      stream = stream.limit(spec.limit) if spec.limit?
      stream = start_condition(spec,stream) if spec.start
      stream = stop_condition(spec,stream) if spec.stop
      stream = stream.order_by(id: order(spec))
      stream
    end

    def normalize_stream_name(specification)
      specification.stream.global? ? EventRepository::SERIALIZED_GLOBAL_STREAM_NAME : specification.stream.name
    end

    def start_condition(spec,stream)
      event_record = Event.find_by(event_id: spec.start, stream: normalize_stream_name(spec))

      criteria_value = event_record.created_at

      if spec.forward?
        stream.where(:created_at.gt => criteria_value)
      else
        stream.where(:created_at.lt => criteria_value)
      end
    end

    def stop_condition(spec,stream)
      event_record = Event.find_by(event_id: spec.stop, stream: normalize_stream_name(spec))

      criteria_value = event_record.created_at

      if spec.forward?
        stream.where(:created_at.lt => criteria_value)
      else
        stream.where(:created_at.gt => criteria_value)
      end
    end

    def build_event_instance(mongoid_record)
      return nil unless mongoid_record

      RubyEventStore::SerializedRecord.new(
        event_id: mongoid_record.event_id,
        metadata: mongoid_record.meta,
        data: mongoid_record.data,
        event_type: mongoid_record.event_type
      )
    end

    def order(spec)
      spec.forward? ? 'asc' : 'desc'
    end
  end
end