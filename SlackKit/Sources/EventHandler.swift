//
// EventHandler.swift
//
// Copyright © 2016 Peter Zignego. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

import Foundation

internal struct EventHandler {
    
    //MARK: - Initial connection
    static func connected() {
        Client.sharedInstance.connected = true
        if let delegate = Client.sharedInstance.slackEventsDelegate {
            delegate.clientConnected()
        }
        Client.sharedInstance.sendMessage("blah blah blah", channelID: Client.sharedInstance.channels.first!.1.id!)
    }
    
    //MARK: - Messages
    
    static func messageSent(event: Event) {
        if let reply = event.replyTo, message = Client.sharedInstance.sentMessages[reply], channel = message.channel, ts = message.ts {
            message.ts = event.ts
            message.text = event.text
            Client.sharedInstance.channels[channel]?.messages[ts] = message
            
            if let delegate = Client.sharedInstance.messageEventsDelegate {
                delegate.messageSent(message)
            }
        }
    }
    
    static func messageReceived(event: Event) {
        if let channel = event.channel, message = event.message, id = channel.id, ts = message.ts {
            Client.sharedInstance.channels[id]?.messages[ts] = message
            
            if let delegate = Client.sharedInstance.messageEventsDelegate {
                delegate.messageReceived(message)
            }
        }
    }
    
    static func messageChanged(event: Event) {
        if let id = event.channel?.id, nested = event.nestedMessage, ts = nested.ts {
            Client.sharedInstance.channels[id]?.messages[ts] = nested
            
            if let delegate = Client.sharedInstance.messageEventsDelegate {
                delegate.messageChanged(nested)
            }
        }
    }
    
    static func messageDeleted(event: Event) {
        if let id = event.channel?.id, key = event.message?.deletedTs {
            let message = Client.sharedInstance.channels[id]?.messages[key]
            Client.sharedInstance.channels[id]?.messages.removeValueForKey(key)
            
            if let delegate = Client.sharedInstance.messageEventsDelegate {
                delegate.messageDeleted(message)
            }
        }
    }
    
    //MARK: - Channels
    static func userTyping(event: Event) {
        if let channelID = event.channel?.id, userID = event.user?.id {
            if let _ = Client.sharedInstance.channels[channelID] {
                if (!Client.sharedInstance.channels[channelID]!.usersTyping.contains(userID)) {
                    Client.sharedInstance.channels[channelID]?.usersTyping.append(userID)
                    
                    if let delegate = Client.sharedInstance.channelEventsDelegate {
                        delegate.userTyping(event.channel, user: event.user)
                    }
                }
            }
            
            /*let timeout = dispatch_time(DISPATCH_TIME_NOW, Int64(5.0 * Double(NSEC_PER_SEC)))
            dispatch_after(timeout, dispatch_get_main_queue()) {
                if let index = Client.sharedInstance.channels[channelID]?.usersTyping.indexOf(userID) {
                    Client.sharedInstance.channels[channelID]?.usersTyping.removeAtIndex(index)
                }
            }*/
        }
    }
    
    static func channelMarked(event: Event) {
        if let channel = event.channel, id = channel.id {
            Client.sharedInstance.channels[id]?.lastRead = event.ts
            
            if let delegate = Client.sharedInstance.channelEventsDelegate {
                delegate.channelMarked(channel, timestamp: event.ts)
            }
        }
        //TODO: Recalculate unreads
    }
    
    static func channelCreated(event: Event) {
        if let channel = event.channel, id = channel.id {
            Client.sharedInstance.channels[id] = channel
            
            if let delegate = Client.sharedInstance.channelEventsDelegate {
                delegate.channelCreated(channel)
            }
        }
    }
    
    static func channelDeleted(event: Event) {
        if let channel = event.channel, id = channel.id {
            Client.sharedInstance.channels.removeValueForKey(id)
            
            if let delegate = Client.sharedInstance.channelEventsDelegate {
                delegate.channelDeleted(channel)
            }
        }
    }
    
    static func channelJoined(event: Event) {
        if let channel = event.channel, id = channel.id {
            Client.sharedInstance.channels[id] = event.channel
            
            if let delegate = Client.sharedInstance.channelEventsDelegate {
                delegate.channelJoined(channel)
            }
        }
    }
    
    static func channelLeft(event: Event) {
        if let channel = event.channel, id = channel.id, userID = Client.sharedInstance.authenticatedUser?.id {
            if let index = Client.sharedInstance.channels[id]?.members.indexOf(userID) {
                Client.sharedInstance.channels[id]?.members.removeAtIndex(index)
                
                if let delegate = Client.sharedInstance.channelEventsDelegate {
                    delegate.channelLeft(channel)
                }
            }
        }
    }
    
    static func channelRenamed(event: Event) {
        if let channel = event.channel, id = channel.id {
            Client.sharedInstance.channels[id]?.name = channel.name
            
            if let delegate = Client.sharedInstance.channelEventsDelegate {
                delegate.channelRenamed(channel)
            }
        }
    }
    
    static func channelArchived(event: Event, archived: Bool) {
        if let channel = event.channel, id = channel.id {
            Client.sharedInstance.channels[id]?.isArchived = archived
            
            if let delegate = Client.sharedInstance.channelEventsDelegate {
                delegate.channelArchived(channel)
            }
        }
    }
    
    static func channelHistoryChanged(event: Event) {
        if let channel = event.channel {
            //TODO: Reload chat history if there are any cached messages before latest
            
            if let delegate = Client.sharedInstance.channelEventsDelegate {
                delegate.channelHistoryChanged(channel)
            }
        }
    }
    
    //MARK: - Do Not Disturb
    static func doNotDisturbUpdated(event: Event) {
        if let dndStatus = event.dndStatus {
            Client.sharedInstance.authenticatedUser?.doNotDisturbStatus = dndStatus
            
            if let delegate = Client.sharedInstance.doNotDisturbEventsDelegate {
                delegate.doNotDisturbUpdated(dndStatus)
            }
        }
    }
    
    static func doNotDisturbUserUpdated(event: Event) {
        if let dndStatus = event.dndStatus, user = event.user, id = user.id {
            Client.sharedInstance.users[id]?.doNotDisturbStatus = dndStatus
            
            if let delegate = Client.sharedInstance.doNotDisturbEventsDelegate {
                delegate.doNotDisturbUserUpdated(dndStatus, user: user)
            }
        }
    }
    
    //MARK: - IM & Group Open/Close
    static func open(event: Event, open: Bool) {
        if let channel = event.channel, id = channel.id {
            Client.sharedInstance.channels[id]?.isOpen = open
            
            if let delegate = Client.sharedInstance.groupEventsDelegate {
                delegate.groupOpened(channel)
            }
        }
    }
    
    //MARK: - Files
    static func processFile(event: Event) {
        if let file = event.file, id = file.id {
            if let comment = file.initialComment, commentID = comment.id {
                if Client.sharedInstance.files[id]?.comments[commentID] == nil {
                    Client.sharedInstance.files[id]?.comments[commentID] = comment
                }
            }
            
            Client.sharedInstance.files[id] = file
            
            if let delegate = Client.sharedInstance.fileEventsDelegate {
                delegate.fileProcessed(file)
            }
        }
    }
    
    static func filePrivate(event: Event) {
        if let file =  event.file, id = file.id {
            Client.sharedInstance.files[id]?.isPublic = false
            
            if let delegate = Client.sharedInstance.fileEventsDelegate {
                delegate.fileMadePrivate(file)
            }
        }
    }
    
    static func deleteFile(event: Event) {
        if let file = event.file, id = file.id {
            if Client.sharedInstance.files[id] != nil {
                Client.sharedInstance.files.removeValueForKey(id)
            }
            
            if let delegate = Client.sharedInstance.fileEventsDelegate {
                delegate.fileDeleted(file)
            }
        }
    }
    
    static func fileCommentAdded(event: Event) {
        if let file = event.file, id = file.id, comment = event.comment, commentID = comment.id {
            Client.sharedInstance.files[id]?.comments[commentID] = comment
            
            if let delegate = Client.sharedInstance.fileEventsDelegate {
                delegate.fileCommentAdded(file, comment: comment)
            }
        }
    }
    
    static func fileCommentEdited(event: Event) {
        if let file = event.file, id = file.id, comment = event.comment, commentID = comment.id {
            Client.sharedInstance.files[id]?.comments[commentID]?.comment = comment.comment
            
            if let delegate = Client.sharedInstance.fileEventsDelegate {
                delegate.fileCommentEdited(file, comment: comment)
            }
        }
    }
    
    static func fileCommentDeleted(event: Event) {
        if let file = event.file, id = file.id, comment = event.comment, commentID = comment.id {
            Client.sharedInstance.files[id]?.comments.removeValueForKey(commentID)
            
            if let delegate = Client.sharedInstance.fileEventsDelegate {
                delegate.fileCommentDeleted(file, comment: comment)
            }
        }
    }
    
    //MARK: - Pins
    static func pinAdded(event: Event) {
        if let id = event.channelID, item = event.item {
            Client.sharedInstance.channels[id]?.pinnedItems.append(item)
            
            if let delegate = Client.sharedInstance.pinEventsDelegate {
                delegate.itemPinned(item, channel: Client.sharedInstance.channels[id])
            }
        }
    }
    
    static func pinRemoved(event: Event) {
        if let id = event.channelID {
            if let pins = Client.sharedInstance.channels[id]?.pinnedItems.filter({$0 != event.item}) {
                Client.sharedInstance.channels[id]?.pinnedItems = pins
            }
            
            if let delegate = Client.sharedInstance.pinEventsDelegate {
                delegate.itemUnpinned(event.item, channel: Client.sharedInstance.channels[id])
            }
        }
    }
    
    //MARK: - Stars
    static func itemStarred(event: Event, star: Bool) {
        if let item = event.item, type = item.type {
            switch type {
            case "message":
                starMessage(item, star: star)
            case "file":
                starFile(item, star: star)
            case "file_comment":
                starComment(item)
            default:
                break
            }
            
            if let delegate = Client.sharedInstance.starEventsDelegate {
                delegate.itemStarred(item, star: star)
            }
        }
    }
    
    static func starMessage(item: Item, star: Bool) {
        if let message = item.message, ts = message.ts, channel = item.channel {
            if let _ = Client.sharedInstance.channels[channel]?.messages[ts] {
                Client.sharedInstance.channels[channel]?.messages[ts]?.isStarred = star
            }
        }
    }
    
    static func starFile(item: Item, star: Bool) {
        if let file = item.file, id = file.id {
            Client.sharedInstance.files[id]?.isStarred = star
            if let stars = Client.sharedInstance.files[id]?.stars {
                if star == true {
                    Client.sharedInstance.files[id]?.stars = stars + 1
                } else {
                    if stars > 0 {
                        Client.sharedInstance.files[id]?.stars = stars - 1
                    }
                }
            }
        }
    }
    
    static func starComment(item: Item) {
        if let file = item.file, id = file.id, comment = item.comment, commentID = comment.id {
            Client.sharedInstance.files[id]?.comments[commentID] = comment
        }
    }
    
    //MARK: - Reactions
    static func addedReaction(event: Event) {
        if let item = event.item, type = item.type, key = event.reaction, userID = event.user?.id {
            switch type {
            case "message":
                if let channel = item.channel, ts = item.ts {
                    if let message = Client.sharedInstance.channels[channel]?.messages[ts] {
                        if (message.reactions[key]) == nil {
                            message.reactions[key] = Reaction(name: event.reaction, user: userID)
                        } else {
                            message.reactions[key]?.users[userID] = userID
                        }
                    }
                }
            case "file":
                if let id = item.file?.id, file = Client.sharedInstance.files[id] {
                    if file.reactions[key] == nil {
                        Client.sharedInstance.files[id]?.reactions[key] = Reaction(name: event.reaction, user: userID)
                    } else {
                        Client.sharedInstance.files[id]?.reactions[key]?.users[userID] = userID
                    }
                }
            case "file_comment":
                if let id = item.file?.id, file = Client.sharedInstance.files[id], commentID = item.fileCommentID {
                    if file.comments[commentID]?.reactions[key] == nil {
                        Client.sharedInstance.files[id]?.comments[commentID]?.reactions[key] = Reaction(name: event.reaction, user: userID)
                    } else {
                        Client.sharedInstance.files[id]?.comments[commentID]?.reactions[key]?.users[userID] = userID
                    }
                }
                break
            default:
                break
            }
            
            if let delegate = Client.sharedInstance.reactionEventsDelegate {
                delegate.reactionAdded(event.reaction, item: event.item)
            }
        }
    }
    
    static func removedReaction(event: Event) {
        if let item = event.item, type = item.type, key = event.reaction, userID = event.user?.id {
            switch type {
            case "message":
                if let channel = item.channel, ts = item.ts {
                    if let message = Client.sharedInstance.channels[channel]?.messages[ts] {
                        if (message.reactions[key]) != nil {
                            message.reactions[key]?.users.removeValueForKey(userID)
                        }
                        if (message.reactions[key]?.users.count == 0) {
                            message.reactions.removeValueForKey(key)
                        }
                    }
                }
            case "file":
                if let itemFile = item.file, id = itemFile.id, file = Client.sharedInstance.files[id] {
                    if file.reactions[key] != nil {
                        Client.sharedInstance.files[id]?.reactions[key]?.users.removeValueForKey(userID)
                    }
                    if Client.sharedInstance.files[id]?.reactions[key]?.users.count == 0 {
                        Client.sharedInstance.files[id]?.reactions.removeValueForKey(key)
                    }
                }
            case "file_comment":
                if let id = item.file?.id, file = Client.sharedInstance.files[id], commentID = item.fileCommentID {
                    if file.comments[commentID]?.reactions[key] != nil {
                        Client.sharedInstance.files[id]?.comments[commentID]?.reactions[key]?.users.removeValueForKey(userID)
                    }
                    if Client.sharedInstance.files[id]?.comments[commentID]?.reactions[key]?.users.count == 0 {
                        Client.sharedInstance.files[id]?.comments[commentID]?.reactions.removeValueForKey(key)
                    }
                }
                break
            default:
                break
            }
            
            if let delegate = Client.sharedInstance.reactionEventsDelegate {
                delegate.reactionAdded(event.reaction, item: event.item)
            }
        }
    }
    
    //MARK: - Preferences
    static func changePreference(event: Event) {
        if let name = event.name {
            Client.sharedInstance.authenticatedUser?.preferences?[name] = event.value
            
            if let delegate = Client.sharedInstance.slackEventsDelegate, value = event.value {
                delegate.preferenceChanged(name, value: value)
            }
        }
    }
    
    //Mark: - User Change
    static func userChange(event: Event) {
        if let user = event.user, id = user.id {
            let preferences = Client.sharedInstance.users[id]?.preferences
            Client.sharedInstance.users[id] = user
            Client.sharedInstance.users[id]?.preferences = preferences
            
            if let delegate = Client.sharedInstance.slackEventsDelegate {
                delegate.userChanged(user)
            }
        }
    }
    
    //MARK: - User Presence
    static func presenceChange(event: Event) {
        if let user = event.user, id = user.id {
            Client.sharedInstance.users[id]?.presence = event.presence
            
            if let delegate = Client.sharedInstance.slackEventsDelegate {
                delegate.presenceChanged(user, presence: event.presence)
            }
        }
    }
    
    //MARK: - Team
    static func teamJoin(event: Event) {
        if let user = event.user, id = user.id {
            Client.sharedInstance.users[id] = user
            
            if let delegate = Client.sharedInstance.teamEventsDelegate {
                delegate.teamJoined(user)
            }
        }
    }
    
    static func teamPlanChange(event: Event) {
        if let plan = event.plan {
            Client.sharedInstance.team?.plan = plan
            
            if let delegate = Client.sharedInstance.teamEventsDelegate {
                delegate.teamPlanChanged(plan)
            }
        }
    }
    
    static func teamPreferenceChange(event: Event) {
        if let name = event.name {
            Client.sharedInstance.team?.prefs?[name] = event.value
            
            if let delegate = Client.sharedInstance.teamEventsDelegate, value = event.value {
                delegate.teamPreferencesChanged(name, value: value)
            }
        }
    }
    
    static func teamNameChange(event: Event) {
        if let name = event.name {
            Client.sharedInstance.team?.name = name
            
            if let delegate = Client.sharedInstance.teamEventsDelegate {
                delegate.teamNameChanged(name)
            }
        }
    }
    
    static func teamDomainChange(event: Event) {
        if let domain = event.domain {
            Client.sharedInstance.team?.domain = domain
            
            if let delegate = Client.sharedInstance.teamEventsDelegate {
                delegate.teamDomainChanged(domain)
            }
        }
    }
    
    static func emailDomainChange(event: Event) {
        if let domain = event.emailDomain {
            Client.sharedInstance.team?.emailDomain = domain
            
            if let delegate = Client.sharedInstance.teamEventsDelegate {
                delegate.teamEmailDomainChanged(domain)
            }
        }
    }
    
    static func emojiChanged(event: Event) {
        //TODO: Call emoji.list here
        
        if let delegate = Client.sharedInstance.teamEventsDelegate {
            delegate.teamEmojiChanged()
        }
    }
    
    //MARK: - Bots
    static func bot(event: Event) {
        if let bot = event.bot, id = bot.id {
            Client.sharedInstance.bots[id] = bot
            
            if let delegate = Client.sharedInstance.slackEventsDelegate {
                delegate.botEvent(bot)
            }
        }
    }
    
    //MARK: - Subteams
    static func subteam(event: Event) {
        if let subteam = event.subteam, id = subteam.id {
            Client.sharedInstance.userGroups[id] = subteam
            
            if let delegate = Client.sharedInstance.subteamEventsDelegate {
                delegate.subteamEvent(subteam)
            }
        }
        
    }
    
    static func subteamAddedSelf(event: Event) {
        if let subteamID = event.subteamID, _ = Client.sharedInstance.authenticatedUser?.userGroups {
            Client.sharedInstance.authenticatedUser?.userGroups![subteamID] = subteamID
            
            if let delegate = Client.sharedInstance.subteamEventsDelegate {
                delegate.subteamSelfAdded(subteamID)
            }
        }
    }
    
    static func subteamRemovedSelf(event: Event) {
        if let subteamID = event.subteamID {
            Client.sharedInstance.authenticatedUser?.userGroups?.removeValueForKey(subteamID)
            
            if let delegate = Client.sharedInstance.subteamEventsDelegate {
                delegate.subteamSelfRemoved(subteamID)
            }
        }
    }
    
    //MARK: - Authenticated User
    static func manualPresenceChange(event: Event) {
        Client.sharedInstance.authenticatedUser?.presence = event.presence
        
        if let delegate = Client.sharedInstance.slackEventsDelegate {
            delegate.manualPresenceChanged(Client.sharedInstance.authenticatedUser, presence: event.presence)
        }
    }
    
}
