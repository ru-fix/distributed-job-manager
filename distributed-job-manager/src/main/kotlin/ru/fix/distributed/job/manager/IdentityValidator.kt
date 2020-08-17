package ru.fix.distributed.job.manager

object IdentityValidator {
    enum class IdentityType{WorkItem, NodeId, JobId }
    private const val MAX_SIZE = 120
    private val PATTERN = "[a-zA-Z0-9._-]+".toRegex()

    fun validate(identityType: IdentityType, identity: String?){
        require(identity.length <= MAX_SIZE) {
            "$identityType $identity is bigger than $MAX_SIZE"
        }
        require(PATTERN.matches(identity)) {
            "$identityType $identity does not match pattern $PATTERN"
        }
    }
}