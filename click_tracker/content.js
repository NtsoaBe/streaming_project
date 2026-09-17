document.addEventListener("click", (event) => {
    const click = {
        x: event.clientX,
        y: event.clientY,
        pageName: document.title,
        url: window.location.href,
        timestamp: Date.now()
    };

    console.log("Sending click:", click);

    fetch("http://localhost:3000/click", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify(click)
    }).catch((error) => {
        console.error("Could not send click:", error);
    });
}, true);
