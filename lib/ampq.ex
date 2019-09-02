defmodule RabbitmqCli.AMQP do
  use GenServer
  require Logger
  use AMQP

  @host "amqp://guest:guest@localhost"
  @queue_name "bol_queue"
  @exchange_name "bol_exchange"

  def start_link do
    GenServer.start_link(__MODULE__, [], [])
  end

  def init(_) do
    {:ok, connection} = Connection.open(@host)
    {:ok, channel} = Channel.open(connection)

    AMQP.Exchange.declare(channel, @exchange_name, :direct)
    {:ok, _} = Queue.declare(channel, @queue_name, durable: true)
    :ok = Queue.bind(channel, @queue_name, @exchange_name)
    Basic.consume(channel, @queue_name)

    {:ok, channel}
  end

  @doc """
    Confirmation sent by the broker after registering this process as a consumer
  """
  def handle_info({:basic_consume_ok, %{consumer_tag: _consumer_tag}}, channel) do
    {:noreply, channel}
  end

  @doc """
    Sent by the broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
  """
  def handle_info({:basic_cancel, %{consumer_tag: _consumer_tag}}, channel) do
    {:stop, :normal, channel}
  end

  @doc """
    Confirmation sent by the broker to the consumer process after a Basic.cancel
  """
  def handle_info({:basic_cancel_ok, %{consumer_tag: _consumer_tag}}, channel) do
    {:noreply, channel}
  end

@doc """
   Sent by the broker when a message is delivered
"""
  def handle_info(
        {:basic_deliver, payload, %{delivery_tag: tag, redelivered: redelivered}},
        channel
      ) do

    spawn fn ->
      IO.puts("handle_info")
      consume(channel, tag, redelivered, payload)
    end

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
