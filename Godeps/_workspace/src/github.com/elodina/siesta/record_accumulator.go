package siesta

import (
	"sync"
	"time"
)

type RecordAccumulatorConfig struct {
	batchSize         int
	totalMemorySize   int
	compressionType   string
	linger            time.Duration
	retryBackoff      time.Duration
	blockOnBufferFull bool
	metrics           map[string]Metric
	time              time.Time
	metricTags        map[string]string
	networkClient     *NetworkClient
}

type RecordBatch struct {
	sync.RWMutex
	batch []*ProducerRecord
}

type RecordAccumulator struct {
	config        *RecordAccumulatorConfig
	networkClient *NetworkClient
	batchSize     int
	batches       map[string]map[int32]*RecordBatch

	addChan      chan *ProducerRecord
	closing      chan bool
	closed       chan bool
	metadataChan chan *RecordMetadata
	records      map[string]map[int32]chan *ProducerRecord
}

func NewRecordAccumulator(config *RecordAccumulatorConfig, metadataChan chan *RecordMetadata) *RecordAccumulator {
	accumulator := &RecordAccumulator{}
	accumulator.config = config
	accumulator.batchSize = config.batchSize
	accumulator.addChan = make(chan *ProducerRecord, 100) //TODO config
	accumulator.batches = make(map[string]map[int32]*RecordBatch)
	accumulator.networkClient = config.networkClient
	accumulator.closing = make(chan bool)
	accumulator.closed = make(chan bool)
	accumulator.metadataChan = metadataChan
	accumulator.records = make(map[string]map[int32]chan *ProducerRecord)

	go accumulator.sender()

	return accumulator
}

func (ra *RecordAccumulator) sender() {
	for {
		select {
		case <-ra.closing:
			ra.cleanup()
			return
		default:
			{
				select {
				case <-ra.closing:
					ra.cleanup()
					return
				case record := <-ra.addChan:
					ra.addRecord(record)
				}
			}
		}
	}
}

func (ra *RecordAccumulator) cleanup() {
	close(ra.addChan)
	ra.flushAll()
	ra.networkClient.close()
	ra.closed <- true
}

func (ra *RecordAccumulator) addRecord(record *ProducerRecord) {
	if ra.batches[record.Topic] == nil {
		ra.batches[record.Topic] = make(map[int32]*RecordBatch)
	}
	if ra.batches[record.Topic][record.partition] == nil {
		ra.createBatch(record.Topic, record.partition)
	}
	ra.records[record.Topic][record.partition] <- record
}

func (ra *RecordAccumulator) createBatch(topic string, partition int32) {
	batch := make([]*ProducerRecord, 0, ra.batchSize)
	ra.batches[topic][partition] = &RecordBatch{batch: batch}
	if ra.records[topic] == nil {
		ra.records[topic] = make(map[int32]chan *ProducerRecord)
	}
	ra.records[topic][partition] = make(chan *ProducerRecord, ra.batchSize)
	go ra.watcher(topic, partition)
}

func (ra *RecordAccumulator) watcher(topic string, partition int32) {
	timeout := time.NewTimer(ra.config.linger)
	for {
		select {
		case record := <-ra.records[topic][partition]:
			batch := ra.batches[record.Topic][record.partition]
			batch.Lock()
			batch.batch = append(batch.batch, record)
			batch.Unlock()
		case <-timeout.C:
			ra.flush(topic, partition)
			timeout.Reset(ra.config.linger)
		case <-ra.closing:
			ra.closing <- true
			timeout.Stop()
			return
		}
		batch := ra.batches[topic][partition]
		batch.RLock()
		lenght := len(batch.batch)
		batch.RUnlock()
		if lenght >= ra.batchSize {
			ra.flush(topic, partition)
			timeout.Reset(ra.config.linger)
		}
	}
	timeout.Stop()
}

func (ra *RecordAccumulator) flush(topic string, partition int32) {
	batch := ra.batches[topic][partition]
	batch.Lock()
	defer batch.Unlock()
	if len(ra.batches[topic][partition].batch) > 0 {
		ra.networkClient.send(topic, partition, batch.batch)
		batch.batch = make([]*ProducerRecord, 0, ra.batchSize)
	}
}

func (ra *RecordAccumulator) flushAll() {
	for topic, partitionBatches := range ra.batches {
		for partition, _ := range partitionBatches {
			ra.flush(topic, partition)
		}
	}
}

func (ra *RecordAccumulator) close() chan bool {
	ra.closing <- true
	return ra.closed
}
