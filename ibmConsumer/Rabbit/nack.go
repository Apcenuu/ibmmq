package Rabbit

type Nack struct {
	Tag      uint64
	Multiple bool
	Requeue  bool
}
