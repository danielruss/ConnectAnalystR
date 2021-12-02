# ConnectAnalystR
R package for DCEG connect analysts

to install from github, you first need the devtools package.
```
install.packages('devtools')
```
then 
```
devtools::install_github("Analyticsphere/ConnectAnalystR")

```

In order to run, you currently need a gcp service account key (given a key, the code knows if you want the production/staging environment).
If you need a key, 
1. Talk to Nicole (if you don't know Nicole, you probably aren't going to get a key) 
2. **She** will make the request for a key.
3. If you get a key, you will keep key the safe on your disk and **never** check it into github.

In the future, I hope to have the OAuth dance working so that you run as a user (with appropriate IAM permission) so you don't need a key.
