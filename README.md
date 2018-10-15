# scala-aws

An AWS Java SDK wrapper that that embraces [referential transparency](https://www.reddit.com/r/scala/comments/8ygjcq/can_someone_explain_to_me_the_benefits_of_io/e2s29ym) 
and composability via [Cats-effect](https://typelevel.org/cats-effect/). It only offers APIs for S3 for now.
  
## Documentation
https://aws.amazon.com/sdk-for-java/

## S3 Environment Variables
These are the expected environment variable by default. However, the key names are overridable.

"AWS_ACCESS_KEY_ID"         - required

"AWS_SECRET_ACCESS_KEY"     - required 

"AWS_S3_BUCKET_NAME"        - required

"AWS_S3_ENDPOINT"           - optional
     
## Script Usage

A `go` wrapper is a proxy for all operations. All dependencies are managed by docker.

Usage: `./go <command> [sub-command]`

```
Available commands are:
    sbt [cmd] SBT commands (http://www.scala-sbt.org/)
    test      Run tests
    build     Build the library jar file
    deploy    Deploy library to artifactory
    nuke      Destroy all your running containers and remove ALL build cache
```     
