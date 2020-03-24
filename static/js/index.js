;(function (win) {
  var socketio = win.io();
  socketio.on('connect', onReady);
  socketio.on('message', onGetMessage);

  function onReady () {
    socketio.emit('message', {
      id: 'status'
    });
  }

  function onGetMessage (message) {
    console.log(message);
    switch (message.id) {
      case 'statusResponse':
        $('#anchorStatusResponsePipelinesCount').text(message.info.pipelines.length);
        $('#statusResponsePipelines').JSONView(message.info.pipelines);
        $('#anchorStatusResponseModulesCount').text(message.info.modules.length);
        $('#statusResponseModules').JSONView(message.info.modules);
        break;
    }
  }
})(this);
