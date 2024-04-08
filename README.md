# Instructions on how to run a wandb server locally

## Pre-reqs

* Docker engine
* wandb

```
pip install wandb
```

### Start wandb

```
wandb server start
```

![image info](./screenshots/screenshot-1.png)

### Go to http://localhost:8080 in your browswer

![image info](./screenshots/screenshot-2.png)

### Click "Login", create an account. 

Use a personal email and don't forget the password!

![image info](./screenshots/screenshot-3.png)

### Click on "Get a free license"

![image info](./screenshots/screenshot-4.png)

### Click on "Get a free license"

![image info](./screenshots/screenshot-5.png)

### Click on "Continue"

![image info](./screenshots/screenshot-6.png)

### Click on "New Organization"

![image info](./screenshots/screenshot-7.png)

### Add an Organization Name then click "Next"

![image info](./screenshots/screenshot-8.png)

### Click "Generate License Key"

![image info](./screenshots/screenshot-9.png)

### Click "Copy License"

![image info](./screenshots/screenshot-10.png)

### Click "Copy"

![image info](./screenshots/screenshot-11.png)

### Go back to localhost:8000, click on "Add license"

![image info](./screenshots/screenshot-12.png)

### Paste license into text box

The page should go from this:

![image info](./screenshots/screenshot-13-2.png)

To this:

![image info](./screenshots/screenshot-13.png)

### Click "Update settings"

![image info](./screenshots/screenshot-14.png)

### Go back to localhost:8000, copy API key

![image info](./screenshots/screenshot-15.png)

### Use key in below python program called wb-run.py

```
import wandb

WANDB_BASE_URL = "http://localhost:8080"
WANDB_API_KEY = "local-ca43659ee79c59fbf3b5f551218c659766038799"
wandb.login(host=WANDB_BASE_URL, key=WANDB_API_KEY)
run = wandb.init(project="foo-project",group="foo-group",job_type='foo-job-type')
run.finish()
```

### Run the following python program

```
python wb-run.py
```

### See run under My projects

![image info](./screenshots/screenshot-16.png)


### The End. You now are ready to integrate wandb into your code and see what you've done locally!
