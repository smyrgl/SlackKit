//////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Websocket.swift
//
//  Created by Dalton Cherry on 7/16/14.
//  Copyright (c) 2014-2015 Dalton Cherry.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
//////////////////////////////////////////////////////////////////////////////////////////////////

import Foundation
import CoreFoundation
#if os(Linux)
    import Glibc
    import SwiftShims
    import CDispatch
#endif

public protocol WebSocketDelegate: class {
    func websocketDidConnect(socket: WebSocket)
    func websocketDidDisconnect(socket: WebSocket, error: NSError?)
    func websocketDidReceiveMessage(socket: WebSocket, text: String)
    func websocketDidReceiveData(socket: WebSocket, data: NSData)
}

public protocol WebSocketPongDelegate: class {
    func websocketDidReceivePong(socket: WebSocket)
}

public class WebSocket : NSObject {
    
    enum CFStreamEvent: UInt {
        case None = 0
        case OpenCompleted = 1
        case HasBytesAvailable = 2
        case CanAcceptBytes = 4
        case ErrorOccured = 8
        case EndEncountered = 16
    }
    
    enum OpCode : UInt8 {
        case ContinueFrame = 0x0
        case TextFrame = 0x1
        case BinaryFrame = 0x2
        //3-7 are reserved.
        case ConnectionClose = 0x8
        case Ping = 0x9
        case Pong = 0xA
        //B-F reserved.
    }
    
    public enum CloseCode : UInt16 {
        case Normal                 = 1000
        case GoingAway              = 1001
        case ProtocolError          = 1002
        case ProtocolUnhandledType  = 1003
        // 1004 reserved.
        case NoStatusReceived       = 1005
        //1006 reserved.
        case Encoding               = 1007
        case PolicyViolated         = 1008
        case MessageTooBig          = 1009
    }
    
    public static let ErrorDomain = "WebSocket"
    
    enum InternalErrorCode : UInt16 {
        // 0-999 WebSocket status codes not used
        case OutputStreamWriteError  = 1
    }
    
    //Queue
    public var queue            = dispatch_queue_create("com.websocket.queue", nil)
    
    var optionalProtocols       : [String]?
    //Constant Values.
    let headerWSUpgradeName     = "Upgrade"
    let headerWSUpgradeValue    = "websocket"
    let headerWSHostName        = "Host"
    let headerWSConnectionName  = "Connection"
    let headerWSConnectionValue = "Upgrade"
    let headerWSProtocolName    = "Sec-WebSocket-Protocol"
    let headerWSVersionName     = "Sec-WebSocket-Version"
    let headerWSVersionValue    = "13"
    let headerWSKeyName         = "Sec-WebSocket-Key"
    let headerOriginName        = "Origin"
    let headerWSAcceptName      = "Sec-WebSocket-Accept"
    let BUFFER_MAX              = 4096
    let FinMask: UInt8          = 0x80
    let OpCodeMask: UInt8       = 0x0F
    let RSVMask: UInt8          = 0x70
    let MaskMask: UInt8         = 0x80
    let PayloadLenMask: UInt8   = 0x7F
    let MaxFrameSize: Int       = 32
    
    class WSResponse {
        var isFin = false
        var code: OpCode = .ContinueFrame
        var bytesLeft = 0
        var frameCount = 0
        var buffer: NSMutableData?
    }
    
    public weak var delegate: WebSocketDelegate?
    public weak var pongDelegate: WebSocketPongDelegate?
    public var onConnect: ((Void) -> Void)?
    public var onDisconnect: ((NSError?) -> Void)?
    public var onText: ((String) -> Void)?
    public var onData: ((NSData) -> Void)?
    public var onPong: ((Void) -> Void)?
    public var headers = [String: String]()
    public var voipEnabled = false
    public var selfSignedSSL = false
    public var isConnected :Bool {
        return connected
    }
    private var url: NSURL
    private var inputStream: CFReadStream?
    private var outputStream: CFWriteStream?
    
    private var isRunLoop = false
    private var connected = false
    private var isCreated = false
    private var writeQueue = dispatch_queue_create("com.websocket.write", nil)
    private var queueGroup = dispatch_group_create()
    private var readStack = [WSResponse]()
    private var inputQueue = [NSData]()
    private var fragBuffer: NSData?
    private var certValidated = false
    private var didDisconnect = false
    
    //used for setting protocols.
    public init(url: NSURL, protocols: [String]? = nil) {
        self.url = url
        optionalProtocols = protocols
    }
    
    ///Connect to the websocket server on a background thread
    public func connect() {
        guard !isCreated else { return }
        
        dispatch_async(queue) { [weak self] in
            self?.didDisconnect = false
        }
        dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT,0)) { [weak self] in
            self?.isCreated = true
            self?.createHTTPRequest()
            self?.isCreated = false
        }
    }
    
    ///write a string to the websocket. This sends it as a text frame.
    public func writeString(str: String) {
        dequeueWrite(str.dataUsingEncoding(NSUTF8StringEncoding)!, code: .TextFrame)
    }
    
    ///write binary data to the websocket. This sends it as a binary frame.
    public func writeData(data: NSData) {
        dequeueWrite(data, code: .BinaryFrame)
    }
    
    //write a   ping   to the websocket. This sends it as a  control frame.
    //yodel a   sound  to the planet.    This sends it as an astroid. http://youtu.be/Eu5ZJELRiJ8?t=42s
    public func writePing(data: NSData) {
        dequeueWrite(data, code: .Ping)
    }
    //private methods below!
    
    //private method that starts the connection
    private func createHTTPRequest() {
        let request = NSURLRequest(URL: url)
        let req = request.mutableCopy() as! NSMutableURLRequest
        req.setValue("websocket", forHTTPHeaderField: "Upgrade")
        req.setValue("Upgrade", forHTTPHeaderField: "Connection")
        if req.valueForHTTPHeaderField("User-Agent") == nil {
            req.setValue("SwiftWebSocket", forHTTPHeaderField: "User-Agent")
        }
        req.setValue("13", forHTTPHeaderField: "Sec-WebSocket-Version")
        
        if req.URL!.port == nil || req.URL!.port!.integerValue == 80 || req.URL!.port!.integerValue == 443  {
            req.setValue(req.URL!.host!, forHTTPHeaderField: "Host")
        } else {
            req.setValue("\(req.URL!.host!):\(req.URL!.port!.integerValue)", forHTTPHeaderField: "Host")
        }
        req.setValue(req.URL!.absoluteString, forHTTPHeaderField: "Origin")
        if optionalProtocols?.count > 0 {
            req.setValue(optionalProtocols?.joinWithSeparator(";"), forHTTPHeaderField: "Sec-WebSocket-Protocol")
        }
        
        let port : Int
        if req.URL!.scheme == "wss" {
            port = req.URL!.port?.integerValue ?? 443
        } else {
            port = req.URL!.port?.integerValue ?? 80
        }
        
        var path = req.URL?.path
        if path == "" {
            path = "/"
        }
        if let q = req.URL!.query {
            if q != "" {
                path?.appendContentsOf("?" + q)
            }
        }
        let get = "GET "
        let http = " HTTP/1.1\r\n"
        var reqs = get+path!+http
        for key in req.allHTTPHeaderFields!.keys {
            if let val = req.valueForHTTPHeaderField(key) {
                reqs += "\(key): \(val)\r\n"
            }
        }
        var keyb = [UInt32](count: 4, repeatedValue: 0)
        for var i = 0; i < 4; i++ {
            keyb[i] = ls_arc4random()
        }
        let rkey = NSData(bytes: keyb, length: 16).base64EncodedStringWithOptions(NSDataBase64EncodingOptions(rawValue: 0))
        reqs += "Sec-WebSocket-Key: \(rkey)\r\n"
        reqs += "\r\n"
        var header = [UInt8]()
        for b in reqs.utf8 {
            header += [b]
        }
        let addr = ["\(req.URL!.host!)", "\(port)"]
        if addr.count != 2 || Int(addr[1]) == nil {
        }

        initStreamsWithData(header, port)
    }

    //generate a websocket key as needed in rfc
    private func generateWebSocketKey() -> String {
        var key = ""
        let seed = 16
        for _ in 0..<seed {
            let uni = UnicodeScalar(UInt32(97 +  ls_arc4random_uniform(25)))
            key += "\(Character(uni))"
        }
        let data = key.dataUsingEncoding(NSUTF8StringEncoding)
        let baseKey = data?.base64EncodedStringWithOptions(NSDataBase64EncodingOptions(rawValue: 0))
        return baseKey!
    }
    
    //Start the stream connection and write the data to the output stream
    private func initStreamsWithData(data: [UInt8], _ port: Int) {
        var readStream: Unmanaged<CFReadStream>?
        var writeStream: Unmanaged<CFWriteStream>?
        #if os(Linux)
            let h = url.host as! CFString
        #else
            let h = url.host
        #endif
        CFStreamCreatePairWithSocketToHost(nil, h, UInt32(port), &readStream, &writeStream)
        inputStream = readStream!.takeRetainedValue()
        outputStream = writeStream!.takeRetainedValue()
        guard let inStream = inputStream, let outStream = outputStream else { return }
        if ["wss", "https"].contains(ls_urlScheme(url.scheme)) {
            CFReadStreamSetProperty(inStream, NSStreamSocketSecurityLevelKey as! CFString, NSStreamSocketSecurityLevelNegotiatedSSL as! CFString)
            CFWriteStreamSetProperty(outStream, NSStreamSocketSecurityLevelKey as! CFString, NSStreamSocketSecurityLevelNegotiatedSSL as! CFString)
        } else {
            certValidated = true //not a https session, so no need to check SSL pinning
        }
        
        isRunLoop = true
        CFReadStreamScheduleWithRunLoop(inStream, CFRunLoopGetCurrent(), kCFRunLoopDefaultMode)
        CFWriteStreamScheduleWithRunLoop(outStream, CFRunLoopGetCurrent(), kCFRunLoopDefaultMode)
        var context = CFStreamClientContext(version: 1, info: nil, retain: nil, release: nil, copyDescription: nil)
        context.info = UnsafeMutablePointer(Unmanaged.passUnretained(self).toOpaque())
        CFReadStreamSetClient(inStream, CFStreamEvent.OpenCompleted.rawValue | CFStreamEvent.HasBytesAvailable.rawValue | CFStreamEvent.EndEncountered.rawValue, { readStream, event, data -> Void in
            let weak = Unmanaged<WebSocket>.fromOpaque(COpaquePointer(data)).takeUnretainedValue()
            weak.procesStream(readStream, event: event)
            print(event)
            }, &context)
        CFReadStreamOpen(inStream)
        CFWriteStreamOpen(outStream)
        CFWriteStreamWrite(outStream, data, data.count)
        while(isRunLoop) {
            let time = NSDate.distantFuture().timeIntervalSinceNow
            CFRunLoopRunInMode(kCFRunLoopDefaultMode, CFTimeInterval(time), false)
        }
    }
    
    //CFStream callback. Processes incoming bytes
    public func procesStream(readStream: CFReadStream, event: CFStreamEventType) {
        #if os(Linux)
            if event == CFStreamEvent.HasBytesAvailable.rawValue {
                processInputStream()
            } else if event == CFStreamEvent.ErrorOccured.rawValue {
                let error = CFReadStreamGetError(readStream)
                disconnectStream(NSError(domain: String(error.domain), code: Int(error.error), userInfo: nil))
            } else if event == CFStreamEvent.EndEncountered.rawValue {
                disconnectStream(nil)
            }
        #else
            if event.rawValue == CFStreamEvent.HasBytesAvailable.rawValue {
                processInputStream()
            } else if event.rawValue == CFStreamEvent.ErrorOccured.rawValue {
                let error = CFReadStreamGetError(readStream)
                disconnectStream(NSError(domain: String(error.domain), code: Int(error.error), userInfo: nil))
            } else if event.rawValue == CFStreamEvent.EndEncountered.rawValue {
                disconnectStream(nil)
            }
        #endif
    }
    
    //disconnect the stream object
    private func disconnectStream(error: NSError?) {
        dispatch_group_wait(queueGroup, DISPATCH_TIME_FOREVER)
        if let stream = inputStream {
            CFReadStreamUnscheduleFromRunLoop(stream, CFRunLoopGetCurrent(), kCFRunLoopDefaultMode)
            CFReadStreamClose(stream)
        }
        if let stream = outputStream {
            CFWriteStreamUnscheduleFromRunLoop(stream, CFRunLoopGetCurrent(), kCFRunLoopDefaultMode)
            CFWriteStreamClose(stream)
        }
        outputStream = nil
        isRunLoop = false
        certValidated = false
        doDisconnect(error)
        connected = false
    }
    
    ///handles the incoming bytes and sending them to the proper processing method
    private func processInputStream() {
        let buf = NSMutableData(capacity: BUFFER_MAX)
        let buffer = UnsafeMutablePointer<UInt8>(buf!.bytes)
        let length = CFReadStreamRead(inputStream, buffer, BUFFER_MAX)
        
        guard length > 0 else { return }
        
        if !connected {
            connected = processHTTP(buffer, bufferLen: length)
            if connected {
                dispatch_async(queue) { [weak self] in
                    guard let s = self else { return }
                    s.onConnect?()
                    s.delegate?.websocketDidConnect(s)
                }
            } else {
                //let response = CFHTTPMessageCreateEmpty(kCFAllocatorDefault, false).takeRetainedValue()
                //CFHTTPMessageAppendBytes(response, buffer, length)
                //let code = CFHTTPMessageGetResponseStatusCode(response)
                //doDisconnect(errorWithDetail("Invalid HTTP upgrade", code: UInt16(code)))
                dispatch_async(queue) { [weak self] in
                    guard let s = self else { return }
                    s.didDisconnect = true
                    s.onDisconnect?(nil)
                    s.delegate?.websocketDidDisconnect(s, error: nil)
                }
            }
        } else {
            var process = false
            if inputQueue.count == 0 {
                process = true
            }
            inputQueue.append(NSData(bytes: buffer, length: length))
            if process {
                dequeueInput()
            }
        }
    }
    ///dequeue the incoming input so it is processed in order
    private func dequeueInput() {
        guard !inputQueue.isEmpty else { return }
        
        let data = inputQueue[0]
        var work = data
        if let fragBuffer = fragBuffer {
            let combine = NSMutableData(data: fragBuffer)
            combine.appendData(data)
            work = combine
            self.fragBuffer = nil
        }
        let buffer = UnsafePointer<UInt8>(work.bytes)
        processRawMessage(buffer, bufferLen: work.length)
        inputQueue = inputQueue.filter{$0 != data}
        dequeueInput()
    }
    ///Finds the HTTP Packet in the TCP stream, by looking for the CRLF.
    private func processHTTP(buffer: UnsafePointer<UInt8>, bufferLen: Int) -> Bool {
        let CRLFBytes = [UInt8(ascii: "\r"), UInt8(ascii: "\n"), UInt8(ascii: "\r"), UInt8(ascii: "\n")]
        var k = 0
        var totalSize = 0
        for i in 0..<bufferLen {
            if buffer[i] == CRLFBytes[k] {
                k += 1
                if k == 3 {
                    totalSize = i + 1
                    break
                }
            } else {
                k = 0
            }
        }
        if totalSize > 0 {
            if validateResponse(buffer, bufferLen: totalSize) {
                totalSize += 1 //skip the last \n
                let restSize = bufferLen - totalSize
                if restSize > 0 {
                    processRawMessage((buffer+totalSize),bufferLen: restSize)
                }
                return true
            }
        }
        return false
    }
    
    ///validates the HTTP is a 101 as per the RFC spec
    private func validateResponse(buffer: UnsafePointer<UInt8>, bufferLen: Int) -> Bool {
        /*let response = CFHTTPMessageCreateEmpty(kCFAllocatorDefault, false).takeRetainedValue()
        CFHTTPMessageAppendBytes(response, buffer, bufferLen)
        if CFHTTPMessageGetResponseStatusCode(response) != 101 {
            return false
        }
        if let cfHeaders = CFHTTPMessageCopyAllHeaderFields(response) {
            let headers = cfHeaders.takeRetainedValue() as NSDictionary
            if let acceptKey = headers[headerWSAcceptName] as? NSString {
                if acceptKey.length > 0 {
                    return true
                }
            }
        }
        return false*/
        return true
    }
    
    ///read a 16 bit big endian value from a buffer
    private static func readUint16(buffer: UnsafePointer<UInt8>, offset: Int) -> UInt16 {
        return (UInt16(buffer[offset + 0]) << 8) | UInt16(buffer[offset + 1])
    }
    
    ///read a 64 bit big endian value from a buffer
    private static func readUint64(buffer: UnsafePointer<UInt8>, offset: Int) -> UInt64 {
        var value = UInt64(0)
        for i in 0...7 {
            value = (value << 8) | UInt64(buffer[offset + i])
        }
        return value
    }
    
    ///write a 16 bit big endian value to a buffer
    private static func writeUint16(buffer: UnsafeMutablePointer<UInt8>, offset: Int, value: UInt16) {
        buffer[offset + 0] = UInt8(value >> 8)
        buffer[offset + 1] = UInt8(value & 0xff)
    }
    
    ///write a 64 bit big endian value to a buffer
    private static func writeUint64(buffer: UnsafeMutablePointer<UInt8>, offset: Int, value: UInt64) {
        for i in 0...7 {
            buffer[offset + i] = UInt8((value >> (8*UInt64(7 - i))) & 0xff)
        }
    }
    
    ///process the websocket data
    private func processRawMessage(buffer: UnsafePointer<UInt8>, bufferLen: Int) {
        let response = readStack.last
        if response != nil && bufferLen < 2  {
            fragBuffer = NSData(bytes: buffer, length: bufferLen)
            return
        }
        if let response = response where response.bytesLeft > 0 {
            var len = response.bytesLeft
            var extra = bufferLen - response.bytesLeft
            if response.bytesLeft > bufferLen {
                len = bufferLen
                extra = 0
            }
            response.bytesLeft -= len
            response.buffer?.appendData(NSData(bytes: buffer, length: len))
            processResponse(response)
            let offset = bufferLen - extra
            if extra > 0 {
                processExtra((buffer+offset), bufferLen: extra)
            }
            return
        } else {
            let isFin = (FinMask & buffer[0])
            let receivedOpcode = OpCode(rawValue: (OpCodeMask & buffer[0]))
            let isMasked = (MaskMask & buffer[1])
            let payloadLen = (PayloadLenMask & buffer[1])
            var offset = 2
            if (isMasked > 0 || (RSVMask & buffer[0]) > 0) && receivedOpcode != .Pong {
                let errCode = CloseCode.ProtocolError.rawValue
                doDisconnect(errorWithDetail("masked and rsv data is not currently supported", code: errCode))
                writeError(errCode)
                return
            }
            let isControlFrame = (receivedOpcode == .ConnectionClose || receivedOpcode == .Ping)
            if !isControlFrame && (receivedOpcode != .BinaryFrame && receivedOpcode != .ContinueFrame &&
                receivedOpcode != .TextFrame && receivedOpcode != .Pong) {
                let errCode = CloseCode.ProtocolError.rawValue
                doDisconnect(errorWithDetail("unknown opcode: \(receivedOpcode)", code: errCode))
                writeError(errCode)
                return
            }
            if isControlFrame && isFin == 0 {
                let errCode = CloseCode.ProtocolError.rawValue
                doDisconnect(errorWithDetail("control frames can't be fragmented", code: errCode))
                writeError(errCode)
                return
            }
            if receivedOpcode == .ConnectionClose {
                var code = CloseCode.Normal.rawValue
                if payloadLen == 1 {
                    code = CloseCode.ProtocolError.rawValue
                } else if payloadLen > 1 {
                    code = WebSocket.readUint16(buffer, offset: offset)
                    if code < 1000 || (code > 1003 && code < 1007) || (code > 1011 && code < 3000) {
                        code = CloseCode.ProtocolError.rawValue
                    }
                    offset += 2
                }
                if payloadLen > 2 {
                    let len = Int(payloadLen-2)
                    if len > 0 {
                        let bytes = UnsafePointer<UInt8>((buffer+offset))
                        let str: NSString? = NSString(data: NSData(bytes: bytes, length: len), encoding: NSUTF8StringEncoding)
                        if str == nil {
                            code = CloseCode.ProtocolError.rawValue
                        }
                    }
                }
                doDisconnect(errorWithDetail("connection closed by server", code: code))
                writeError(code)
                return
            }
            if isControlFrame && payloadLen > 125 {
                writeError(CloseCode.ProtocolError.rawValue)
                return
            }
            var dataLength = UInt64(payloadLen)
            if dataLength == 127 {
                dataLength = WebSocket.readUint64(buffer, offset: offset)
                offset += sizeof(UInt64)
            } else if dataLength == 126 {
                dataLength = UInt64(WebSocket.readUint16(buffer, offset: offset))
                offset += sizeof(UInt16)
            }
            if bufferLen < offset || UInt64(bufferLen - offset) < dataLength {
                fragBuffer = NSData(bytes: buffer, length: bufferLen)
                return
            }
            var len = dataLength
            if dataLength > UInt64(bufferLen) {
                len = UInt64(bufferLen-offset)
            }
            var data: NSData!
            if len < 0 {
                len = 0
                data = NSData()
            } else {
                data = NSData(bytes: UnsafePointer<UInt8>((buffer+offset)), length: Int(len))
            }
            if receivedOpcode == .Pong {
                dispatch_async(queue) { [weak self] in
                    guard let s = self else { return }
                    s.onPong?()
                    s.pongDelegate?.websocketDidReceivePong(s)
                }
                
                let step = Int(offset+numericCast(len))
                let extra = bufferLen-step
                if extra > 0 {
                    processRawMessage((buffer+step), bufferLen: extra)
                }
                return
            }
            var response = readStack.last
            if isControlFrame {
                response = nil //don't append pings
            }
            if isFin == 0 && receivedOpcode == .ContinueFrame && response == nil {
                let errCode = CloseCode.ProtocolError.rawValue
                doDisconnect(errorWithDetail("continue frame before a binary or text frame", code: errCode))
                writeError(errCode)
                return
            }
            var isNew = false
            if response == nil {
                if receivedOpcode == .ContinueFrame  {
                    let errCode = CloseCode.ProtocolError.rawValue
                    doDisconnect(errorWithDetail("first frame can't be a continue frame",
                        code: errCode))
                    writeError(errCode)
                    return
                }
                isNew = true
                response = WSResponse()
                response!.code = receivedOpcode!
                response!.bytesLeft = Int(dataLength)
                response!.buffer = NSMutableData(data: data)
            } else {
                if receivedOpcode == .ContinueFrame  {
                    response!.bytesLeft = Int(dataLength)
                } else {
                    let errCode = CloseCode.ProtocolError.rawValue
                    doDisconnect(errorWithDetail("second and beyond of fragment message must be a continue frame",
                        code: errCode))
                    writeError(errCode)
                    return
                }
                response!.buffer!.appendData(data)
            }
            if let response = response {
                response.bytesLeft -= Int(len)
                response.frameCount += 1
                response.isFin = isFin > 0 ? true : false
                if isNew {
                    readStack.append(response)
                }
                processResponse(response)
            }
            
            let step = Int(offset+numericCast(len))
            let extra = bufferLen-step
            if extra > 0 {
                processExtra((buffer+step), bufferLen: extra)
            }
        }
        
    }
    
    ///process the extra of a buffer
    private func processExtra(buffer: UnsafePointer<UInt8>, bufferLen: Int) {
        if bufferLen < 2 {
            fragBuffer = NSData(bytes: buffer, length: bufferLen)
        } else {
            processRawMessage(buffer, bufferLen: bufferLen)
        }
    }
    
    ///process the finished response of a buffer
    private func processResponse(response: WSResponse) -> Bool {
        if response.isFin && response.bytesLeft <= 0 {
            if response.code == .Ping {
                let data = response.buffer! //local copy so it is perverse for writing
                dequeueWrite(data, code: OpCode.Pong)
            } else if response.code == .TextFrame {
                let str: NSString? = NSString(data: response.buffer!, encoding: NSUTF8StringEncoding)
                if str == nil {
                    writeError(CloseCode.Encoding.rawValue)
                    return false
                }
                
                dispatch_async(queue) { [weak self] in
                    guard let s = self else { return }
                    s.onText?(String(str!))
                    s.delegate?.websocketDidReceiveMessage(s, text: String(str!))
                }
            } else if response.code == .BinaryFrame {
                let data = response.buffer! //local copy so it is perverse for writing
                dispatch_async(queue) { [weak self] in
                    guard let s = self else { return }
                    s.onData?(data)
                    s.delegate?.websocketDidReceiveData(s, data: data)
                }
            }
            readStack.removeLast()
            return true
        }
        return false
    }
    
    ///Create an error
    private func errorWithDetail(detail: String, code: UInt16) -> NSError {
        #if os(Linux)
            var details = [String: Any]?()
        #else
            var details = [String: String]?()
        #endif
        details?[NSLocalizedDescriptionKey] =  detail
        return NSError(domain: WebSocket.ErrorDomain, code: Int(code), userInfo: details)
    }
    
    ///write a an error to the socket
    private func writeError(code: UInt16) {
        let buf = NSMutableData(capacity: sizeof(UInt16))
        let buffer = UnsafeMutablePointer<UInt8>(buf!.bytes)
        WebSocket.writeUint16(buffer, offset: 0, value: code)
        dequeueWrite(NSData(bytes: buffer, length: sizeof(UInt16)), code: .ConnectionClose)
    }
    ///used to write things to the stream
    private func dequeueWrite(data: NSData, code: OpCode) {
        guard isConnected else { return }
        dispatch_group_async(queueGroup, writeQueue) { [weak self] in
            //stream isn't ready, let's wait
            guard let s = self else { return }
            var offset = 2
            let bytes = UnsafeMutablePointer<UInt8>(data.bytes)
            let dataLength = data.length
            let frame = NSMutableData(capacity: dataLength + s.MaxFrameSize)
            let buffer = UnsafeMutablePointer<UInt8>(frame!.mutableBytes)
            buffer[0] = s.FinMask | code.rawValue
            if dataLength < 126 {
                buffer[1] = CUnsignedChar(dataLength)
            } else if dataLength <= Int(UInt16.max) {
                buffer[1] = 126
                WebSocket.writeUint16(buffer, offset: offset, value: UInt16(dataLength))
                offset += sizeof(UInt16)
            } else {
                buffer[1] = 127
                WebSocket.writeUint64(buffer, offset: offset, value: UInt64(dataLength))
                offset += sizeof(UInt64)
            }
            buffer[1] |= s.MaskMask
            let maskKey = UnsafeMutablePointer<UInt8>(buffer + offset)
            //SecRandomCopyBytes(kSecRandomDefault, Int(sizeof(UInt32)), maskKey)
            offset += sizeof(UInt32)
            
            for i in 0..<dataLength {
                buffer[offset] = bytes[i] ^ maskKey[i % sizeof(UInt32)]
                offset += 1
            }
            var total = 0
            while true {
                if !s.isConnected {
                    break
                }
                guard let outStream = s.outputStream else { break }
                let writeBuffer = UnsafePointer<UInt8>(frame!.bytes+total)
                let len = CFWriteStreamWrite(outStream, writeBuffer, offset-total)
                if len < 0 {
                    var error: NSError?
                    let streamError = CFWriteStreamGetError(outStream)
                    error = NSError(domain: String(streamError.domain), code: Int(streamError.error), userInfo: nil)
                    let errCode = InternalErrorCode.OutputStreamWriteError.rawValue
                    error = s.errorWithDetail("output stream error during write", code: errCode)
                    s.doDisconnect(error)
                    break
                } else {
                    total += len
                }
                if total >= offset {
                    break
                }
            }
            
        }
    }
    
    ///used to preform the disconnect delegate
    private func doDisconnect(error: NSError?) {
        guard !didDisconnect else { return }
        
        dispatch_async(queue) { [weak self] in
            guard let s = self else { return }
            s.didDisconnect = true
            s.onDisconnect?(error)
            s.delegate?.websocketDidDisconnect(s, error: error)
        }
    }
    
    //Linux utils
    func ls_arc4random_uniform(upperBound: UInt32) -> UInt32 {
        #if os(Linux)
            return _swift_stdlib_arc4random_uniform(upperBound)
        #else
            return arc4random_uniform(upperBound)
        #endif
    }
    
    func ls_arc4random() -> UInt32 {
        #if os(Linux)
            return _swift_stdlib_arc4random()
        #else
            return arc4random()
        #endif
    }
    
    func ls_urlScheme(scheme: String?) -> String {
        return scheme!
    }
    
}
