package server

import (
	"net/http"
	"notifyx/models"
	"notifyx/services/queuer"

	"github.com/gin-gonic/gin"
)

type NotifyX struct {
	Queue *queuer.TaskQueue
}

func InitServer(queue *queuer.TaskQueue) *gin.Engine {
	handler := &NotifyX{Queue: queue}
	router := gin.Default()

	router.POST("/enqueue", handler.EnqueueTask)

	return router
}

func (n *NotifyX) EnqueueTask(c *gin.Context) {
	var task models.Task
	if err := c.ShouldBindJSON(&task); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}
	n.Queue.Enqueue(&task)
	c.JSON(http.StatusAccepted, gin.H{"message": "Task enqueued"})
}
