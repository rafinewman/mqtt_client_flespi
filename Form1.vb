Imports MQTTnet
Imports MQTTnet.Client
Imports MQTTnet.Protocol
Imports System.Text


Public Class Form1
    Private Sub Form1_Load(sender As Object, e As EventArgs) Handles MyBase.Load
        mmqt_start()
    End Sub


    Dim mqClient As MqttClient
    Dim mqFactory As New MqttFactory

    Sub mmqt_start()
        Connect("client_id_dev_msgs", "mqtt.flespi.io", "IVc4iYV4PP9wCUjIWIGS1sSSaABfNvajgccpYVhEAe00Y8zIG5oxl4dltEo6Zoe4", "").Wait()
        'Publish("topic/volume", "20").Wait()
        'Subscribe("flespi/interval/gw/calcs/+/devices/+/created").Wait()
        Subscribe("flespi/message/gw/devices/#").Wait()
    End Sub

    Async Function Connect(Client As String, Server As String, User As String, Password As String) As Task
        mqClient = CType(mqFactory.CreateMqttClient(), MqttClient)
        mqClient.UseApplicationMessageReceivedHandler(AddressOf MessageRecieved)
        mqClient.UseDisconnectedHandler(AddressOf ConnectionClosed)
        mqClient.UseConnectedHandler(AddressOf ConnectionOpened)

        Dim Options As New Options.MqttClientOptionsBuilder
        Options.WithClientId(Client).WithTcpServer(Server, 1883).WithCredentials(User, Password).WithCleanSession(False)
        Await mqClient.ConnectAsync(Options.Build).ConfigureAwait(False)
    End Function

    Async Function Publish(Topic As String, Payload As String) As Task
        Await mqClient.PublishAsync(Topic, Payload).ConfigureAwait(False)
    End Function

    Async Function Subscribe(Topic As String) As Task
        Await mqClient.SubscribeAsync(Topic, MqttQualityOfServiceLevel.AtLeastOnce).ConfigureAwait(False)
    End Function

    Private Sub MessageRecieved(e As MqttApplicationMessageReceivedEventArgs)
        'MessageBox.Show("Topic: " & e.ApplicationMessage.Topic & Chr(10) & Chr(13) & "Payload: " & Encoding.UTF8.GetString(e.ApplicationMessage.Payload))
        EDTUtils20.LogFiles.Log(Encoding.UTF8.GetString(e.ApplicationMessage.Payload))
    End Sub

    Private Sub ConnectionClosed(e As Disconnecting.MqttClientDisconnectedEventArgs)
        MessageBox.Show("Connection closed.")
    End Sub

    Private Sub ConnectionOpened(e As Connecting.MqttClientConnectedEventArgs)
        MessageBox.Show("Connection opened.")
    End Sub
End Class
