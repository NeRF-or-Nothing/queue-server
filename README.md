# RabbitMQ Queue Server

This is a simple queue server to help with load distribution and balancing our renderers, so that we can easily spin up/down renderers as needed.

Any rendering job that isn't ACKed (say the machine shut down or dropped its connection) will be resubmitted, which is very helpful to have automated.

Demo Video of synchronous vs asynchronous queue handling (top 2 windows are synchronous, bottom 2 windows are asynchronous, sync takes 10 seconds whereas async takes 5.4 seconds):

![Video](https://i.imgur.com/OnpWNbo.gif)
</br>

# Examples

Redirect here for a folder of basic examples I followed in the docs [Examples](./examples/)
