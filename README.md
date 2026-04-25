# idea
this languge is made for a small colabrative game between many people.
it is purpusfully missing functions and loops so we can have termination at all cost.

each tick we send messages out and recive them, unanswered messages get discarded.
note that this can cause deadlocks so special care needs to be used to make tasks excuting between recives, and each send then queues up the next tasks after it.

# syntax

if a {
	send "hi" to "left" //can also be send("hi","left")
} else {
	b = recive from "jake" //can also be recive("jake")
}

if(len(b)>2){
	set(b\[2:\])
}

