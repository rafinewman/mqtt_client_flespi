Imports MQTTnet
Imports MQTTnet.Client
Imports MQTTnet.Protocol
Imports System.Text
Imports Newtonsoft.Json
Imports Newtonsoft.Json.Linq
Imports System.Messaging
Imports System.Configuration.ConfigurationManager
Imports System.IO
Imports System.Threading


Public Class Form1
    Private Sub Form1_Load(sender As Object, e As EventArgs) Handles MyBase.Load

        Dim bRecoverableQ As Boolean
        Try
            MAX_Q_4_CS_V3 = CInt(AppSettings("TARGET_CS_MAX_NUMBER_Q"))
        Catch ex As Exception
            MAX_Q_4_CS_V3 = 10
        End Try
        MAX_Q_4_CS_V3 = Math.Max(2, MAX_Q_4_CS_V3)
        TargetCommServerHost = AppSettings("TARGET_CS_HOST_IP")
        ReDim q_TargetsRtlang(MAX_Q_4_CS_V3)
        Try
            bRecoverableQ = (AppSettings("RECOVERABLE_Q").ToLower <> "no")
        Catch ex As Exception
            bRecoverableQ = True
        End Try
        Dim url_prefix As String = "FormatName:DIRECT=TCP:" + TargetCommServerHost + "\private$\rtlang"
        For i As Integer = 0 To MAX_Q_4_CS_V3
            q_TargetsRtlang(i) = New MessageQueue(url_prefix & Strings.Right("00" & i, 2), False)
            q_TargetsRtlang(i).DefaultPropertiesToSend.Recoverable = bRecoverableQ
        Next

        globals.init(".\app_data\")
        mmqt_start()

        ' refuel from ruptela
        'ProcessPayload("{""begin"":1643621600,""end"":1643622388,""duration"":788,""ident"":""860037053946266"",""fuel.first.val"":2122,""fuel.last.val"":3224,""fuel.delta.val"":1102,""external.powersource.voltage"":28.582,""position.direction"":52.6,""position.latitude"":6.544717,""position.longitude"":3.354882,""position.satellites"":22,""position.speed"":0,""vehicle.mileage"":183.461,""gsm.signal.dbm"":31,""protocol.id"":5,""timestamp"":1643622393.381894,""id"":2}")
    End Sub

    Dim mqClient As MqttClient
    Dim mqFactory As New MqttFactory

    Sub mmqt_start()
        'MX
        'Connect("client_id_dev_msgs", "mqtt.flespi.io", "IVc4iYV4PP9wCUjIWIGS1sSSaABfNvajgccpYVhEAe00Y8zIG5oxl4dltEo6Zoe4", "").Wait()
        'QA
        Try
            Connect("client_id_dev_intervals", "mqtt.flespi.io", "TWcsRYShmHGOEAXoiZvf68b8zVQX2UTLFW1mIHsUcYwGmjUv6fA3cCZBnEmdm5MR", "").Wait()
            'Publish("topic/volume", "20").Wait()
            'Subscribe("flespi/interval/gw/calcs/+/devices/+/created,updated,deactivated,activated").Wait()
            Subscribe("flespi/interval/gw/calcs/+/devices/+/created,updated").Wait()
            'Subscribe("flespi/interval/gw/calcs/+/devices/+/updated").Wait()
            'Subscribe("flespi/interval/gw/calcs/+/devices/+/deactivated ").Wait()
            'Subscribe("flespi/interval/gw/calcs/+/devices/+/activated").Wait()
            'Subscribe("flespi/state/gw/calcs/598009/devices/+/last").Wait()
            'Subscribe("flespi/message/gw/devices/#").Wait()
            EDTUtils4x.LogFiles.Log("mmqt_start. ThreadID=" & Thread.CurrentThread.ManagedThreadId)
        Catch ex As Exception
            EDTUtils4x.LogFiles.Log(ex)
        End Try
    End Sub

    Async Function Connect(Client As String, Server As String, User As String, Password As String) As Task
        Try
            mqClient = CType(mqFactory.CreateMqttClient(), MqttClient)
            mqClient.UseApplicationMessageReceivedHandler(AddressOf MessageRecieved)
            mqClient.UseDisconnectedHandler(AddressOf ConnectionClosed)
            mqClient.UseConnectedHandler(AddressOf ConnectionOpened)

            Dim Options As New Options.MqttClientOptionsBuilder
            Options.WithClientId(Client).WithTcpServer(Server, 1883).WithCredentials(User, Password).WithCleanSession(False)
            Await mqClient.ConnectAsync(Options.Build).ConfigureAwait(False)

        Catch ex As Exception
            EDTUtils4x.LogFiles.Log(ex)
        End Try
    End Function

    Async Function Publish(Topic As String, Payload As String) As Task
        Await mqClient.PublishAsync(Topic, Payload).ConfigureAwait(False)
    End Function

    Async Function Subscribe(Topic As String) As Task
        Await mqClient.SubscribeAsync(Topic, MqttQualityOfServiceLevel.AtMostOnce).ConfigureAwait(False)
    End Function

    Private nCounter As Integer = 0
    Private t1 As Long = 0
    Private Sub MessageRecieved(e As MqttApplicationMessageReceivedEventArgs)
        Dim s() As String
        Dim topic_event As String = ""
        Dim pl As String = ""

        e.AutoAcknowledge = True

        'MessageBox.Show("Topic: " & e.ApplicationMessage.Topic & Chr(10) & Chr(13) & "Payload: " & Encoding.UTF8.GetString(e.ApplicationMessage.Payload))
        If e.ApplicationMessage.Payload IsNot Nothing Then
            pl = Encoding.UTF8.GetString(e.ApplicationMessage.Payload)
        End If
        EDTUtils4x.LogFiles.Log("MessageRecieved. ThreadID=" & Thread.CurrentThread.ManagedThreadId)
        EDTUtils4x.LogFiles.Log(e.ApplicationMessage.Topic & " : " & pl)
        If pl = "" Then Return
        'parse to get the exact calculator event
        Try
            s = e.ApplicationMessage.Topic.Split("/")
            topic_event = s(s.Count - 1)
        Catch ex As Exception
            EDTUtils4x.LogFiles.Log(ex)
        End Try

        If {"created", "updated"}.Contains(topic_event) Then
            ProcessPayload(pl)
        End If

        If nCounter = 0 Then t1 = Now.Ticks

        nCounter += 1
        If nCounter = 1000 Then EDTUtils4x.LogFiles.Log("1000 msgs: " & (Now.Ticks - t1) \ 10000 & " ms")
        If nCounter = 2000 Then EDTUtils4x.LogFiles.Log("2000 msgs: " & (Now.Ticks - t1) \ 10000 & " ms")

    End Sub

    Private Function ProcessPayload(pl As String) As Integer

        Dim j As JToken = JObject.Parse(pl)
        Debug.WriteLine(j.Count)

        'For Each jp As JProperty In j
        '    Debug.WriteLine(jp.Name)
        'Next

        Dim EventTime As DateTime = Nothing

        Dim engine_rpm As String = ""
        Dim engine_hours As String = ""
        Dim odometer As String = ""
        'Dim delta_odometer As String = ""
        Dim servertimeStamp As String
        Dim fuel_sensor_last_val As Integer = -1
        Dim fuel_sensor_first_val As Integer = -1
        Dim fuel_level_last_liters As Decimal = -1
        Dim fuel_level_first_liters As Decimal = -1
        Dim fuel_level_last_pct As Decimal = -1
        'Dim fuel_level_first_pct As Decimal = -1
        Dim refuel_delta_liters As Decimal = -1
        Dim refuel_delta_pct As Decimal = -1
        Dim interval_begin_time As Integer
        Dim calc_type As String = ""
        Dim DeviceID As String
        Dim x As String 'rtlang xml
        Dim final_event_code As Integer = 0

        servertimeStamp = CDate("1970-1-1").AddSeconds(j("timestamp").ToString).ToString("yyyy-MM-dd HH:mm:ss")
        DeviceID = j("ident").ToString
        calc_type = j("calc.type").ToString
        ''!!!  check device id , if empty then ignore message !!!
        x = "<IMsg><Hr><OC>IM1</OC><CS>777</CS><MRT>" + servertimeStamp + "</MRT><DI>" + DeviceID + "</DI><FLSPI>" & calc_type & "</FLSPI></Hr><By>"

        For Each jp As JProperty In j
            Dim p As String = jp.Name
            Dim v As String = jp.Value

            Select Case p
                Case "position.direction"
                    x += "<BE><N>P10</N><V>" + v + "</V></BE>"
                Case "position.longitude"
                    x += "<BE><N>P6</N><V>" + v + "</V></BE>"
                Case "position.latitude"
                    x += "<BE><N>P7</N><V>" + v + "</V></BE>"
                Case "position.satellites"
                    x += "<BE><N>P11</N><V>" + v + "</V></BE>"
                Case "position.speed"
                    x += "<BE><N>P9</N><V>" + v + "</V></BE>"
                Case "end"
                    EventTime = CDate("1970-1-1").AddSeconds(v)
                Case "begin"
                    interval_begin_time = v
                Case "external.powersource.voltage"
                    x += "<BE><N>P14</N><V>" + v + "</V></BE>"
                Case "gsm.signal.level", "gsm.signal.dbm"
                    x += "<BE><N>P30</N><V>" + v + "</V></BE>"
                Case "vehicle.mileage"
                    If odometer = "" Then odometer = v
                Case "can.vehicle.mileage"
                    odometer = v    'this value overides other values
                Case "fuel.last.val"
                    fuel_sensor_last_val = v
                Case "fuel.first.val"
                    fuel_sensor_first_val = v
                Case "can.engine.rpm"
                    engine_rpm = v
                Case "engine.rpm"
                    If engine_rpm = "" Then engine_rpm = v
                Case "can.engine.motorhours"
                    engine_hours = v
                Case "engine.motorhours"
                    If engine_hours = "" Then engine_hours = v
            End Select
        Next

        x += "<BE><N>P1</N><V>" + EventTime.ToString("yyyy-MM-dd") + "</V></BE>"
        x += "<BE><N>P2</N><V>" + EventTime.ToString("HH:mm:ss") + "</V></BE>"

        If odometer > "" Then
            x += "<BE><N>P20</N><V>" + odometer + "</V></BE>"
        End If

        Try
            'fuel converison table
            If fuel_sensor_last_val > -1 AndAlso globals.vehicles_fuel_tbl.ContainsKey(DeviceID) Then
                Dim t(,) As Integer = globals.vehicles_fuel_tbl(DeviceID)
                Dim tank_size_liters As Decimal = t(t.GetLength(0) - 1, 1)
                Dim loop_done As Integer = 0
                For k As Integer = 0 To t.GetLength(0) - 1
                    If fuel_level_last_liters = -1 AndAlso fuel_sensor_last_val < t(k, 0) Then
                        fuel_level_last_liters = t(k - 1, 1) + (fuel_sensor_last_val - t(k - 1, 0)) / (t(k, 0) - t(k - 1, 0)) * (t(k, 1) - t(k - 1, 1))
                        loop_done += 1
                    End If
                    If fuel_level_first_liters = -1 AndAlso fuel_sensor_first_val < t(k, 0) Then
                        fuel_level_first_liters = t(k - 1, 1) + (fuel_sensor_first_val - t(k - 1, 0)) / (t(k, 0) - t(k - 1, 0)) * (t(k, 1) - t(k - 1, 1))
                        loop_done += 1
                    End If
                    If loop_done = 2 Then Exit For
                Next
                If fuel_level_last_liters = -1 Then fuel_level_last_liters = tank_size_liters
                If fuel_level_first_liters = -1 Then fuel_level_first_liters = tank_size_liters
                refuel_delta_liters = fuel_level_last_liters - fuel_level_first_liters
                refuel_delta_pct = 100.0 * refuel_delta_liters / tank_size_liters
                fuel_level_last_pct = 100.0 * fuel_level_last_liters / tank_size_liters
                Select Case calc_type
                    Case "refuel"
                        If refuel_delta_pct > 0 And refuel_delta_pct < 11 Then
                            'log small refuel record
                            EDTUtils4x.LogFiles.Log(String.Format("small refuel pct {0:0.0}% , {1:0.0}L", refuel_delta_pct, refuel_delta_liters))
                            Return 0
                        End If
                        final_event_code = 204
                    Case "drain"
                        If refuel_delta_pct < 0 And refuel_delta_pct > 6 Then
                            'log small refuel record
                            EDTUtils4x.LogFiles.Log(String.Format("small drain pct {0:0.0}% , {1:0.0}L", refuel_delta_pct, refuel_delta_liters))
                            Return 0
                        End If
                        final_event_code = 203
                End Select

            End If

            'gps status
            x += "<BE><N>P5</N><V>2</V></BE>"
            If fuel_level_last_pct > -1 Then x += "<BE><N>P28</N><V>" & fuel_level_last_pct.ToString("0.0") & "</V></BE>"
            x += "<BE><N>P18</N><V>" & final_event_code & "</V></BE><BE><N>P19</N><V>" & Math.Abs(refuel_delta_pct).ToString("0.00") & "</V></BE><BE><N>P2537</N><V>" & Math.Abs(refuel_delta_liters).ToString("0.0") & "</V></BE>"

            If engine_rpm > "" Then x += "<BE><N>P21</N><V>" & engine_rpm & "</V></BE>"
            If engine_hours > "" Then x += "<BE><N>P22</N><V>" & CInt(engine_hours) * 60 & "</V></BE>"
            Select Case j("protocol.id")
                Case "5", "37"
                    'Ruptela
                    x += "<BE><N>P104</N><V>1011</V></BE><BE><N>P102</N><V>5</V></BE>"
                Case "14"
                    'Teltonika
                    x += "<BE><N>P104</N><V>1019</V></BE><BE><N>P102</N><V>14</V></BE>"
                Case "23"
                    'ERM
                    x += "<BE><N>P104</N><V>42</V></BE><BE><N>P102</N><V>1</V></BE>"
                Case "33"
                    'Queclink  -> Ubiko 3   ?????
                    x += "<BE><N>P104</N><V>45</V></BE><BE><N>P102</N><V>1</V></BE>"
            End Select

            x += "<BE><N>P9003</N><V>" & interval_begin_time & "</V></BE></By></IMsg>"

            Debug.WriteLine(x)

            Dim qID As Integer = HashDeviceid2Number(DeviceID, MAX_Q_4_CS_V3)
            Send2Queue(x, qID)
            EDTUtils4x.LogFiles.Log("Sent: " & x)
        Catch ex As Exception
            EDTUtils4x.LogFiles.Log(ex)
        End Try

        Return 0
    End Function

    Private MAX_Q_4_CS_V3 As Integer = 10
    Private TargetCommServerHost As String
    Private q_TargetsRtlang(1) As MessageQueue

    Private Function HashDeviceid2Number(deviceID As String, maxNumber As Integer) As Integer
        Dim a As Long = 0
        Dim i As Integer

        'v2.92 - rafi: when device id is missing, select a random Q 
        If deviceID.Trim = "" Then Return Now.Millisecond Mod maxNumber

        For i = 0 To deviceID.Length - 1
            a += Asc(deviceID.Chars(i))
        Next
#If DEBUG Then
        Return a Mod maxNumber
        'Return Now.Millisecond Mod MAX_Q_4_CS_V3
#Else
            Return a Mod maxNumber
#End If

    End Function

    Private Function Send2Queue(msg As String, qID As Integer) As Integer
        Try
            'FormatName:DIRECT=TCP:10.0.11.60\private$\cmxflt1

            If msg.Trim = "" Then Return 2

            Dim oMsg As New Messaging.Message
            Dim writer As New IO.StreamWriter(oMsg.BodyStream)
            writer.Write(msg)
            writer.Flush()
            q_TargetsRtlang(qID).Send(oMsg)

            Return 0
            'Throw New Exception
        Catch ex As Exception
            System.Diagnostics.Debug.WriteLine("FlespiController.Send2Queue failed:" + vbCrLf + vbCrLf + "qid=" & qID & vbCrLf + vbCrLf + ex.ToString)
            EDTUtils4x.LogFiles.Log(ex)
        End Try
        Return 1
    End Function


    Private Sub ConnectionClosed(e As Disconnecting.MqttClientDisconnectedEventArgs)
        'MessageBox.Show("Connection closed.")
        EDTUtils4x.LogFiles.Log("Disconnet reason: " & e.Reason.ToString)
        If e.Exception IsNot Nothing Then EDTUtils4x.LogFiles.Log(e.Exception)
        Thread.Sleep(613)
        mmqt_start()
    End Sub

    Private Sub ConnectionOpened(e As Connecting.MqttClientConnectedEventArgs)
        EDTUtils4x.LogFiles.Log("Connection opened. ThreadID=" & Thread.CurrentThread.ManagedThreadId)
    End Sub
End Class


Public Class globals
    Public Shared vehicles_fuel_tbl As New SortedList(Of String, Array)

    Public Shared Sub init(path As String)
        EDTUtils4x.LogFiles.SingleLogFile = True
        'vehicles_fuel_tbl.Add("860037053946266", {{0, 0}, {204, 18}, {429, 36}, {619, 54}, {797, 72}, {961, 90}, {1099, 108}, {1289, 126}, {1422, 144}, {1569, 162}, {1715, 180}, {1861, 198}, {2007, 216}, {2154, 234}, {2304, 252}, {2457, 270}, {2615, 288}, {2779, 306}, {2953, 324}, {3145, 342}, {3365, 360}, {3654, 380}})
        Dim l As String
        ' HttpRuntime.AppDomainAppPath
        Dim r As New StreamReader(path + "\FuelLevelConversion.txt")
        'Dim r As StreamReader = New StreamReader("c:\temp\flespi\FuelLevelConversion.txt")
        l = r.ReadLine
        While l IsNot Nothing
            'parse line
            Dim s1 As String() = l.Split(":")
            Dim s2 As String() = s1(1).Split(";")
            Dim a(s2.Length - 1, 1) As Integer
            For k As Integer = 0 To s2.Length - 1
                Dim s3 As String() = s2(k).Split(",")
                a(k, 0) = s3(0)
                a(k, 1) = s3(1)
            Next
            vehicles_fuel_tbl.Add(s1(0), a)
            l = r.ReadLine
        End While


    End Sub
End Class