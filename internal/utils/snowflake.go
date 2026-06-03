package utils

import "github.com/bwmarrin/snowflake"

var node *snowflake.Node

func InitSnowflake(workerID int64) error {
	n, err := snowflake.NewNode(workerID)
	if err != nil {
		return err
	}
	node = n
	return nil
}

func GenerateID() int64 {
	return node.Generate().Int64()
}
