package build

import (
	cref "github.com/pip-services3-gox/pip-services3-commons-gox/refer"
	"github.com/pip-services3-gox/pip-services3-messaging-gox/build"
	cqueues "github.com/pip-services3-gox/pip-services3-messaging-gox/queues"
	"github.com/pip-services3-gox/pip-services3-mqtt-gox/queues"
)

// MqttMessageQueueFactory are creates MqttMessageQueue components by their descriptors.
// Name of created message queue is taken from its descriptor.
//
// See Factory
// See MqttMessageQueue
type MqttMessageQueueFactory struct {
	build.MessageQueueFactory
}

// NewMqttMessageQueueFactory method are create a new instance of the factory.
func NewMqttMessageQueueFactory() *MqttMessageQueueFactory {
	c := MqttMessageQueueFactory{
		MessageQueueFactory: *build.InheritMessageQueueFactory(),
	}

	mqttQueueDescriptor := cref.NewDescriptor("pip-services", "message-queue", "mqtt", "*", "1.0")

	c.Register(mqttQueueDescriptor, func(locator interface{}) interface{} {
		name := ""
		descriptor, ok := locator.(*cref.Descriptor)
		if ok {
			name = descriptor.Name()
		}
		return c.CreateQueue(name)
	})

	return &c
}

// Creates a message queue component and assigns its name.
//
// Parameters:
//   - name: a name of the created message queue.
func (c *MqttMessageQueueFactory) CreateQueue(name string) cqueues.IMessageQueue {
	queue := queues.NewMqttMessageQueue(name)

	if c.Config != nil {
		queue.Configure(c.Config)
	}
	if c.References != nil {
		queue.SetReferences(c.References)
	}

	return queue
}
