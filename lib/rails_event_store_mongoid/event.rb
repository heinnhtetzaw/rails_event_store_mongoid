require 'mongoid'

module RailsEventStoreMongoid
  class Event
    include ::Mongoid::Document
    include ::Mongoid::Timestamps::Created::Short

    store_in collection: 'event_store_events'

    field :stream, type: String
    field :event_id, type: String
    field :event_type, type: String
    field :meta, type: Hash
    field :data, type: Hash
    field :position, type: Integer

    field :ts, type: BSON::Timestamp, default: -> { BSON::Timestamp.new(0, 0) }

    index({stream: 1, position: 1}, {unique: true})
    index({stream: 1, event_id: 1}, {unique: true})
    index(event_id: 1)
    index({stream: 1, ts: 1}, {unique: true})

    def self.has_duplicate?(serialized_record, stream_name, linking_event_to_another_stream)
      if linking_event_to_another_stream
        Event.where(event_id: serialized_record.event_id).count > 1
      else
        Event.where(event_id: serialized_record.event_id, stream: stream_name).count > 0
      end
    end
  end
end
