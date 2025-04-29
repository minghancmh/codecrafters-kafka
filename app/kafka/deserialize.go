package kafka

// import (
// 	"github.com/codecrafters-io/kafka-starter-go/app/kafka/api"
// 	"github.com/codecrafters-io/kafka-starter-go/app/kafka/logger"
// )

// var log = logger.Log

// func Deserialize[T any](bytestream []byte, dest *T) {
// 	switch v := any(dest).(type) {
// 	case *api.DescribeTopicPartitionsRequest:
// 		log("Deserializing TopicPartitionsRequest")
// 		api.DeserializeTopicPartitionsRequest(bytestream, v)

// 	case *api.APIVersionsRequest:
// 		log("Deserializing APIVersionsRequest")
// 		api.DeserializeAPIVersionsRequest(bytestream, v)

// 	}
// }
