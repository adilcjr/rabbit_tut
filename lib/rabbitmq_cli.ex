defmodule Rabbitmq.CLI do
  def main(args \\ []) do
    Consumer.start_link
  end
end
