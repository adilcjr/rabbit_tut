defmodule RabbitmqCli.AMQP do
  use GenServer
  require Logger
  use AMQP

  @host "amqp://guest:guest@localhost"
  @reconnect_interval 10_000
  @queue_name "bol_queue"
  @exchange_name "bol_ex"

  def start_link do
    GenServer.start_link(__MODULE__, [], [])
  end

  def init(_) do
    {:ok, connection} = Connection.open(@host)
    {:ok, channel} = Channel.open(connection)

    Exchange.direct(channel, @exchange, durable: true)
    {:ok, _} = Queue.declare(channel, @queue_name, exclusive: true)
    :ok = Queue.bind(channel, @queue_name, @exchange_name)
    Basic.consume(channel, @queue_name)

    Basic.qos(channel, prefetch_count: 10)
    {:ok, channel}
  end

  # Confirmation sent by the broker after registering this process as a consumer
  def handle_info({:basic_consume_ok, %{consumer_tag: _consumer_tag}}, channel) do
    {:noreply, channel}
  end

  # Sent by the broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
  def handle_info({:basic_cancel, %{consumer_tag: _consumer_tag}}, channel) do
    {:stop, :normal, channel}
  end

  # Confirmation sent by the broker to the consumer process after a Basic.cancel
  def handle_info({:basic_cancel_ok, %{consumer_tag: _consumer_tag}}, channel) do
    {:noreply, channel}
  end

  # Sent by the broker when a message is delivered
  def handle_info(
        {:basic_deliver, payload, %{delivery_tag: tag, redelivered: redelivered}},
        channel
      ) do
    # Payload comes in form of String, and contains product id and requested quantity
    # For example, "1.1500"
    # We then process the order, depending on actual quantity the product has

    # spawn(fn ->
      IO.puts("handle_info")
      consume(channel, tag, redelivered, payload)

    {:noreply, channel}
  end

  defp consume(channel, tag, redelivered, payload) do
    IO.puts("Consuming")
    :ok = Basic.ack(channel, tag)
    IO.puts("Receive ----> #{payload}")
  rescue
    exception ->
      :ok = Basic.reject(channel, tag, requeue: not redelivered)
      IO.puts("Error converting #{payload}")
  end
end
