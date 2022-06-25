# RabbitMQ Queue Server

This is a simple queue server to help with load distribution and balancing our renderers, so that we can easily spin up/down renderers as needed.

Any rendering job that isn't ACKed (say the machine shut down or dropped its connection) will be resubmitted, which is very helpful to have automated.

[Examples](./examples/)