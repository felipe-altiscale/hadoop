/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
(function() {
  "use strict";

    var userSettings = { userId: sessionStorage.getItem("userId") };

    var pwd = function() {
        return current_directory;
    };
    window.pwd = pwd;

    angular.module( 'fileBrowserApp', [  ] )

          .controller( 'LoginCtrl', [ '$scope', function( $scope ) {
              $scope.userSettings = userSettings;
              $scope.login = function() {

                // HTML5 session Storage is a hack. It retains the value for the key throughout the session.
                sessionStorage.setItem("userId", userSettings.userId);
              };
          } ] )

          .controller( 'FileBrowserMainCtrl', [ '$scope', function( $scope ) {
              $scope.dir = pwd();
          } ] )

          .controller( 'FileBrowserModalCtrl', [ '$scope', function( $scope ) {

              $scope.getDir = function() {
                  return current_directory;
              };
              $scope.getPath = function() {
                  return current_path;
              };

              $scope.setPermission = function( abs_path ) {
                  var p = 0;

                  if( $( '#perm-ur' ).prop( 'checked' ) )
                      p += 400;
                  if( $( '#perm-uw' ).prop( 'checked' ) )
                      p += 200;
                  if( $( '#perm-ux' ).prop( 'checked' ) )
                      p += 100;
                  if( $( '#perm-gr' ).prop( 'checked' ) )
                      p += 40;
                  if( $( '#perm-gw' ).prop( 'checked' ) )
                      p += 20;
                  if( $( '#perm-gx' ).prop( 'checked' ) )
                      p += 10;
                  if( $( '#perm-or' ).prop( 'checked' ) )
                      p += 4;
                  if( $( '#perm-ow' ).prop( 'checked' ) )
                      p += 2;
                  if( $( '#perm-ox' ).prop( 'checked' ) )
                      p += 1;

                  setPermission( abs_path, p );
                  browse_directory( current_directory );
              };

              $scope.perm = { };

              $scope.fileInfo = {
                  permission: 555
              };

          } ] )

          .controller( 'FileUploadModalCtrl', [ '$scope', function( $scope ) {
          } ] )

  ;

  // The chunk size of tailing the files, i.e., how many bytes will be shown
  // in the preview.
  var TAIL_CHUNK_SIZE = 32768;
  var helpers = {
    'helper_to_permission': function(chunk, ctx, bodies, params) {
      var p = ctx.current().permission;
      var dir = ctx.current().type == 'DIRECTORY' ? 'd' : '-';
      var symbols = [ '---', '--x', '-w-', '-wx', 'r--', 'r-x', 'rw-', 'rwx' ];
      var vInt = parseInt(p, 8);
      var sticky = (vInt & (1 << 9)) != 0;

      var res = "";
      for (var i = 0; i < 3; ++i) {
	res = symbols[(p % 10)] + res;
	p = Math.floor(p / 10);
      }

      if (sticky) {
        var otherExec = (vInt & 1) == 1;
        res = res.substr(0, res.length - 1) + (otherExec ? 't' : 'T');
      }

      chunk.write(dir + res);
      return chunk;
    },

    'helper_to_acl_bit': function(chunk, ctx, bodies, params) {
      if (ctx.current().aclBit) {
        chunk.write('+');
      }
      return chunk;
    }
  };

  var base = dust.makeBase(helpers);
  var current_directory = "";
  var current_path = "";

  function show_err_msg(msg) {
    $('#alert-panel-body').html(msg);
    $('#alert-panel').show();
  }

  $(window).bind('hashchange', function () {
    $('#alert-panel').hide();

    var dir = window.location.hash.slice(1);
    if(dir == "") {
      dir = "/";
    }
    if(current_directory != dir) {
      browse_directory(dir);
    }
  });

  function network_error_handler(url) {
    return function (jqxhr, text, err) {
      switch(jqxhr.status) {
        case 401:
          var msg = '<p>Authentication failed when trying to open ' + url + ': Unauthrozied.</p>';
          break;
        case 403:
          if(jqxhr.responseJSON !== undefined && jqxhr.responseJSON.RemoteException !== undefined) {
            var msg = '<p>' + jqxhr.responseJSON.RemoteException.message + "</p>";
            break;
          }
          var msg = '<p>Permission denied when trying to open ' + url + ': ' + err + '</p>';
          break;
        case 404:
          var msg = '<p>Path does not exist on HDFS or WebHDFS is disabled.  Please check your path or enable WebHDFS</p>';
          break;
        default:
          var msg = '<p>Failed to retreive data from ' + url + ': ' + err + '</p>';
        }
      show_err_msg(msg);
    };
  }

  function append_path(prefix, s) {
    var l = prefix.length;
    var p = l > 0 && prefix[l - 1] == '/' ? prefix.substring(0, l - 1) : prefix;
    return p + '/' + s;
  }

  function get_response(data, type) {
    return data[type] !== undefined ? data[type] : null;
  }

  function get_response_err_msg(data) {
    var msg = data.RemoteException !== undefined ? data.RemoteException.message : "";
    return msg;
  }

  function loadPermissions( p ) {
      $('#perm-input' ).val( p );

      var perms = p;
      var uPerms = Math.floor( perms / 100 );
      perms = perms - ( uPerms * 100 );
      var gPerms = Math.floor( perms / 10 );
      perms = perms - ( gPerms * 10 );
      var oPerms = perms;

      if( Math.floor( uPerms / 4 ) ) {
          $( '#perm-ur' ).prop( 'checked', true );
          uPerms -= 4;
      }
      else
          $( '#perm-ur' ).prop( 'checked', false );
      if( Math.floor( uPerms / 2 ) ) {
          $( '#perm-uw' ).prop( 'checked', true );
          uPerms -= 2;
      }
      else
          $( '#perm-uw' ).prop( 'checked', false );
      if( Math.floor( uPerms ) ) {
          $( '#perm-ux' ).prop( 'checked', true );
      }
      else
          $( '#perm-ux' ).prop( 'checked', false );


      if( Math.floor( gPerms / 4 ) ) {
          $( '#perm-gr' ).prop( 'checked', true );
          gPerms -= 4;
      }
      else
          $( '#perm-gr' ).prop( 'checked', false );
      if( Math.floor( gPerms / 2 ) ) {
          $( '#perm-gw' ).prop( 'checked', true );
          gPerms -= 2;
      }
      else
          $( '#perm-gw' ).prop( 'checked', false );
      if( Math.floor( gPerms ) ) {
          $( '#perm-gx' ).prop( 'checked', true );
      }
      else
          $( '#perm-gx' ).prop( 'checked', false );

      if( Math.floor( oPerms / 4 ) ) {
          $( '#perm-or' ).prop( 'checked', true );
          oPerms -= 4;
      }
      else
          $( '#perm-or' ).prop( 'checked', false );
      if( Math.floor( oPerms / 2 ) ) {
          $( '#perm-ow' ).prop( 'checked', true );
          oPerms -= 2;
      }
      else
          $( '#perm-ow' ).prop( 'checked', false );
      if( Math.floor( oPerms ) ) {
          $( '#perm-ox' ).prop( 'checked', true );
      }
      else
          $( '#perm-ox' ).prop( 'checked', false );

  }

  function view_file_details(path, abs_path) {
    current_path = abs_path;

      $.get('/webhdfs/v1' + abs_path + '?op=GETFILESTATUS', function(data) {
          var d = get_response(data, "FileStatus");
          if (d === null) {
              show_err_msg(get_response_err_msg(data));
              return;
          }

          loadPermissions( d.permission );
      });

    function show_block_info(blocks) {
      var menus = $('#file-info-blockinfo-list');
      menus.empty();

      menus.data("blocks", blocks);
      menus.change(function() {
        var d = $(this).data('blocks')[$(this).val()];
        if (d === undefined) {
          return;
        }

        dust.render('block-info', d, function(err, out) {
          $('#file-info-blockinfo-body').html(out);
        });

      });
      for (var i = 0; i < blocks.length; ++i) {
        var item = $('<option value="' + i + '">Block ' + i + '</option>');
        menus.append(item);
      }
      menus.change();
    }

    var url = '/webhdfs/v1' + abs_path + '?op=GET_BLOCK_LOCATIONS';
    $.ajax({"url": url, "crossDomain": true}).done(function(data) {
      var d = get_response(data, "LocatedBlocks");
      if (d === null) {
        show_err_msg(get_response_err_msg(data));
        return;
      }

      $('#file-info-tail').hide();
      $('#file-info-title').text("File information - " + path);

      var download_url = '/webhdfs/v1' + abs_path + '?op=OPEN';

      $('#file-info-download').attr('href', download_url);
      $('#file-info-preview').click(function() {
        var offset = d.fileLength - TAIL_CHUNK_SIZE;
        var url = offset > 0 ? download_url + '&offset=' + offset : download_url;
        $.get(url, function(t) {
          $('#file-info-preview-body').val(t);
          $('#file-info-tail').show();
        }, "text").error(network_error_handler(url));
      });

      if (d.fileLength > 0) {
        show_block_info(d.locatedBlocks);
        $('#file-info-blockinfo-panel').show();
      } else {
        $('#file-info-blockinfo-panel').hide();
      }
      $('#file-info').modal();
    }).error(network_error_handler(url));
  }

  function setPermission( abs_path, permissionMask ) {

      // PUT /webhdfs/v1/<path>?op=SETPERMISSION&permission=<permission>

      var url = '/webhdfs/v1' + abs_path
          + '?op=SETPERMISSION'
          + '&permission=' + permissionMask
          + '&user.name=' + userSettings.userId
      ;
      $.ajax( { type: 'PUT', url: url, "crossDomain": true}).done(function(data) {
    		// handle response
            var d = get_response(data, "FileStatuses");
            if (d === null) {
 //               show_err_msg(get_response_err_msg(data));
                return;
            }

        }
      ).error(network_error_handler(url));
  }

  function browse_directory(dir) {
    var stuff = []

    $.get('/webhdfs/v1' + dir + '?op=GETFILESTATUS', function(data) {
      var d = get_response(data, "FileStatus");
      if (d === null) {
        show_err_msg(get_response_err_msg(data));
        return;
      }
    });

    var url = '/webhdfs/v1' + dir + '?op=LISTSTATUS';
    $.get(url, function(data) {
      var d = get_response(data, "FileStatuses");
      if (d === null) {
        show_err_msg(get_response_err_msg(data));
        return;
      }
      d.FileStatus = stuff.concat(d.FileStatus)

      current_directory = dir;
      $('.current_directory' ).html( makeBreadcrumb( pwd() ) );
      $('#directory').val(dir);
      window.location.hash = dir;
      dust.render('explorer', base.push(d), function(err, out) {
        $('#panel').html(out);

        $('.explorer-browse-links').click(function() {
          var type = $(this).attr('inode-type');
          var path = $(this).attr('inode-path');
          var abs_path = append_path(current_directory, path);
          if(abs_path == '') {
            abs_path = '/';
          }
          if (type == 'DIRECTORY') {
            browse_directory(abs_path);
          } else {
            view_file_details(path, abs_path);
          }

        });
      });
    }).error(network_error_handler(url));
  }

    function makeBreadcrumb( pathString ) {
        if( pathString.charAt( pathString.length - 1 ) == '/' )
            pathString = pathString.substr( 0, pathString.length - 1 );
        var path = pathString.split( '\/' );

        var res = '/ ';
        var prefix = '';
        var temp = [ ];
        for( var i = 1; i < path.length; i++ ) {
            temp[ i ] = prefix + '/' + path[ i ];
            prefix = temp[ i ];
            res += '<a href="/exp2.html#' + temp[ i ] + '">' + path[ i ] + '</a>' +
                ( i < path.length - 1 ? ' / ' : '' );
        }
        return res;
    }

  function init() {
    dust.loadSource(dust.compile($('#tmpl-explorer').html(), 'explorer'));
    dust.loadSource(dust.compile($('#tmpl-block-info').html(), 'block-info'));

    var b = function() { browse_directory($('#directory').val()); };
    $('#btn-nav-directory').click(b);
    var dir = window.location.hash.slice(1);
    if(dir == "") {
      window.location.hash = "/";
    } else {
      browse_directory(dir);
    }
  }

  init();

  $('#create-directory').on('show.bs.modal', function(event) {
    var pwd = window.pwd();
    if( pwd.slice(-1) != '/' ) {
      pwd = pwd + '/';
    }

    var modal = $(this)
    $('#new_directory_pwd').html(pwd);
    $('#create-directory-button').on('click', function () {
      $(this).prop('disabled', true);
      $(this).button('complete');

      var dir = pwd + $('#new_directory').val();

      mkdir(
        dir,
        null,
        function(data) {
          modal.modal('hide');
          browse_directory(dir);
        },
        function(data) {
          modal.modal('hide');
        })
    })
  });

  $('#upload-file').on('show.bs.modal', function(event) {
    var pwd = window.pwd();
    if( pwd.slice(-1) != '/' ) {
      pwd = pwd + '/';
    }

    var modal = $(this)
    $('#upload-file-button').on('click', function() {
      $(this).prop('disabled', true);
      $(this).button('complete');

      var file = $('#upload-file-input').prop('files')[0];

      var url = '/webhdfs/v1' + pwd + file.name + '?op=CREATE&user.name=' + userSettings.userId;

      return $.ajax({ 
        type: 'PUT',
        url: url,
        data: file,
        processData: false,
        contentType: false,
        crossDomain: true
      }).done(function(data) {
        modal.modal('hide');
        browse_directory(pwd);
      }).error(network_error_handler(url));
    });
  });

  function mkdir(dir, perms, doneFunc, errFunc) {
      if( perms == null) {
        webhdfs(dir, "MKDIRS", {}, doneFunc, errFunc)
      } else {
        webhdfs(dir, "MKDIRS", { "permission": perms }, doneFunc, errFunc);
      }
  }

  function webhdfs(path,operation,options,doneFunc,errFunc) {
    var url = '/webhdfs/v1' + path + '?op=' + operation + '&user.name=' + userSettings.userId;

    $.each(options, function( id, val) {
      url += "&" + id + "=" + val;
    });

    return $.ajax({ 
      type: 'PUT',
      url: url,
      "crossDomain": true
    }).done(function(data) {
      doneFunc(data)
    }).error(network_error_handler(url));
  }
})();
