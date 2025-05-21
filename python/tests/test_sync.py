import glide

def test():
     r = glide.GlideClient.create(glide.GlideClientConfiguration([glide.NodeAddress("localhost", 6379)]))
     print(r.set("goo", "ge"))
     print(r.get("goo"))


test()
